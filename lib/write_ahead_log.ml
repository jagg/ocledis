open Base

(** Write all operations as the come in a sequential
    file. Periodically write a checkpoint and flush the WAL. It will
    handle the updates for a given shard. I will need an interface of
    the storage, independent of Eio, if possible, so that I can swap
    different approaches in the Server *)


type t = {
  table : Storage_hashtbl.t;
  wal_path : string;
  checkpoint_path : string;
  mutable operations : int;
}

let max_wal_size = 10

let get_num channel =
  let buffer = Bytes.create 4 in
  let _ = In_channel.input channel buffer 0 4 in
  Stdlib.Bytes.get_int32_be buffer 0

;;

let get_str channel =
  let len = Int.of_int32_exn @@ get_num channel in
  let buffer = Bytes.create len in
  let _ = In_channel.input channel buffer 0 len in
  Stdlib.Bytes.to_string buffer

 ;;


let store_value channel = function
  | Model.Num n ->
    (** The type of int32 is 0 *)
    Out_channel.output_byte channel 0;
    let buffer = Bytes.create 4 in
    let _ = Encoding.push_int32_exn 0 buffer n in
    Out_channel.output channel buffer 0 4
  | Model.String s ->
    (** The type of string is 1 *)
    Out_channel.output_byte channel 1;
    let len = String.length s in
    let buffer = Bytes.create (4 + len) in
    let _ = Encoding.push_str_exn 0 buffer s in
    Out_channel.output channel buffer 0 (Bytes.length buffer)

;;

(** As of now, both Keys and Values are the same, but that may change,
    I should probably clean up this code duplication *)
let store_key channel = function
  | Model.Key.Num n ->
    Out_channel.output_byte channel 0;
    let buffer = Bytes.create 4 in
    let _ = Encoding.push_int32_exn 0 buffer n in
    Out_channel.output channel buffer 0 4
  | Model.Key.String s ->
    Out_channel.output_byte channel 1;
    let buffer = Bytes.create @@ (String.length s + 4) in
    let _ = Encoding.push_str_exn 0 buffer s in
    Out_channel.output channel buffer 0 (Bytes.length buffer)

;;
let load_data table path =
  In_channel.with_open_gen [Open_binary] 0o666 path
    (fun channel ->
       let rec read () =
         match In_channel.input_byte channel with
         | None -> () (** EOF *)
         | Some input_type ->
           let key = match input_type with
             | 0 -> Model.Key.Num (get_num channel)
             | 1 -> Model.Key.String (get_str channel)
             | _ -> raise (Failure "Type not Supported!")
           in
           match In_channel.input_byte channel with
           | None -> () (** EOF *)
           | Some input_type ->
             let value = match input_type with
               | 0 -> Model.Num (get_num channel)
               | 1 -> Model.String (get_str channel)
               | _ -> raise (Failure "Type not Supported!")
             in
             let _ = Storage_hashtbl.put table.table ~key ~value in
             read ()
       in
       read ()
    )


let load_wal table = load_data table table.wal_path

let save_checkpoint table =
  Out_channel.with_open_gen [Open_binary; Open_creat; Open_trunc; Open_append] 0o666 table.checkpoint_path
    (fun out ->
       Storage_hashtbl.iter table.table ~f:(fun ~key ~data:value ->
           Stdlib.Printf.printf "I got something";
           store_key out key;
           store_value out value;
           Out_channel.flush out;
         );
    )

let load_checkpoint table = load_data table table.checkpoint_path


;;

let create () =
  let table = {
    table = Storage_hashtbl.create ();
    wal_path = "./storage.bin";
    checkpoint_path = "./checkpoint.bin";
    operations = 0;
  } in
  if Stdlib.Sys.file_exists table.checkpoint_path then load_checkpoint table;
  if Stdlib.Sys.file_exists table.wal_path then
    let _ = load_wal table in
    save_checkpoint table; 
    Stdlib.Sys.remove table.wal_path;
  else ();
  table
    
let iter table ~f =
  Storage_hashtbl.iter table.table ~f

let put table ~key ~value =
  table.operations <- table.operations + 1;
  Out_channel.with_open_gen [Open_binary; Open_creat; Open_append] 0o666 table.wal_path
    (fun wal ->
       store_key wal key;
       store_value wal value;
       Out_channel.flush wal;
       Storage_hashtbl.put table.table ~key ~value;
    );
  if table.operations > max_wal_size then
    let _ = save_checkpoint table in
    Stdlib.Sys.remove table.wal_path;
    table.operations <- 0;



;;

let get table ~key = Storage_hashtbl.get table.table ~key
