open! Base

type mode =
  | Leader
  | Follower
[@@deriving sexp]
  
type config = {
  replica : Replica_store.replica_config;
  disk : Disk_store.disk_config;
  mode : mode;
}
[@@deriving sexp]

type t = {
  mem : Memory_store.t;
  disk : Disk_store.t;
  mutable op_count : int;
}

let default_config = {
  replica = {
    replicas = [ ("127.0.0.1", 7777) ];
    quorum = 2;
  };
  disk = {
    wal_path = "./wal.bin";
    checkpoint_path = "checkpoint.bin";
  };
  mode = Leader;
}


let make (config: config) =
  let mem = Memory_store.make () in
  {
    mem = mem;
    disk = Disk_store.start config.disk mem;
    op_count = 0;
  }

let get store key =
  Memory_store.get store.mem key

let update store op =
  let open Or_error in
  Disk_store.process store.disk op >>= fun () ->
  store.op_count <- store.op_count + 1;
  (if store.op_count > 5 then
     let () = store.op_count <- 0 in
     Disk_store.checkpoint store.disk store.mem
   else
     Ok ()) >>= fun () ->
  let () = Memory_store.update store.mem op in
  Ok ()
