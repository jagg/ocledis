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
  repl : Replica_store.t;
  config : config;
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
  let repl = Replica_store.make config.replica in
  {
    mem;
    disk = Disk_store.start config.disk mem;
    repl;
    config;
    op_count = 0;
  }

let get store key =
  Memory_store.get store.mem key

let update store op sw net =
  let open Or_error in
  Disk_store.process store.disk op
  >>= fun () -> match store.config.mode with
  | Leader -> Replica_store.update store.repl op sw net
  | Follower -> Ok ()
  >>= fun () ->
  Memory_store.update store.mem op;
  Ok ()
  >>= fun () ->
  store.op_count <- store.op_count + 1;
  (if store.op_count > 5 then
     let () = store.op_count <- 0 in
     Disk_store.checkpoint store.disk store.mem
   else
     Ok ()) 
