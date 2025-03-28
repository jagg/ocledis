open Base

(** Write all operations as the come in a sequential
    file. Periodically write a checkpoint and flush the WAL. It will
    handle the updates for a given shard. I will need an interface of
    the storage, independent of Eio, if possible, so that I can swap
    different approaches in the Server *)


type 'v t = {
  table : 'v Storage_hashtbl.t;
  wal : Out_channel.t;
}

let create () = {
  table = Storage_hashtbl.create ();
  wal = Out_channel.open_gen [Open_binary; Open_creat; Open_append] 777 "./storage.bin";
}

let put table ~key ~value =
  Out_channel.output_string table.wal "hi\n";
  Out_channel.flush table.wal;
  Storage_hashtbl.put table.table ~key ~value;

;;

let get table ~key = Storage_hashtbl.get table.table ~key
    
