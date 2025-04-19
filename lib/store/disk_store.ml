open! Base

type disk_config = {
  wal_path : string;
  checkpoint_path : string;
}
[@@deriving sexp]

type t = {
  config : disk_config;
}

let get_num channel =
  let buffer = Bytes.create 4 in
  let _ = In_channel.input channel buffer 0 4 in
  Stdlib.Bytes.get_int32_be buffer 0

let get_str channel =
  let len = Int.of_int32_exn @@ get_num channel in
  let buffer = Bytes.create len in
  let _ = In_channel.input channel buffer 0 len in
  Stdlib.Bytes.to_string buffer

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

let load_data_exn mem path =
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
             let _ = Memory_store.update mem (Set (key, value)) in
             read ()
       in
       read ()
    )

let load_wal_exn disk mem = load_data_exn mem disk.config.wal_path

let checkpoint disk mem =
  Or_error.try_with @@ fun () ->
  Out_channel.with_open_gen [Open_binary; Open_creat; Open_trunc; Open_append] 0o666 disk.config.checkpoint_path
    (fun out ->
       Memory_store.iter mem (fun op ->
           match op with
           | Set (key,value) ->
             store_key out key;
             store_value out value;
             (** TODO This is not safe, it should be calling fsync, flush does not guarantee persistence
                 https://transactional.blog/how-to-learn/disk-io
             *)
             Out_channel.flush out;
           | Delete _ -> ()
         );
       Stdlib.Sys.remove disk.config.wal_path;
    )

let load_checkpoint mem disk = load_data_exn mem disk.config.checkpoint_path

let process disk (op:Model.update_op) =
  Or_error.try_with @@ fun () ->
  Out_channel.with_open_gen [Open_binary; Open_creat; Open_append] 0o666 disk.config.wal_path
    (fun wal ->
       match op with
       | Set (key,value) ->
         store_key wal key;
         store_value wal value;
         Out_channel.flush wal;
       | Delete _ -> failwith "Not supported"
    )

let start config mem =
  let disk = {
    config = config;
  } in
  Eio.traceln "Starting Disk store with %s and %s" config.wal_path config.checkpoint_path;
  if Stdlib.Sys.file_exists config.checkpoint_path then load_checkpoint mem disk;
  if Stdlib.Sys.file_exists config.wal_path then
    load_wal_exn disk mem
  else ();
  disk
 
