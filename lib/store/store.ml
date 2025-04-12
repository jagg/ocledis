open! Base

type mode =
  | Leader
  | Follower
[@@deriving sexp]
  
type replica_config = {
  replicas : (string * int) list;
  quorum : int
}
[@@deriving sexp]

type disk_config = {
  wal_path : string;
  checkpoint_path : string;
}
[@@deriving sexp]

type config = {
  replica : replica_config;
  disk : disk_config;
  mode : mode;
}
[@@deriving sexp]

type update_op =
  | Set of Model.Key.t * Model.value
  | Delete of Model.Key.t
[@@deriving sexp]

type t = {
  config : config;
  mem : Memory_store.t;
  disk : Disk_store.t;
}

let get store key =
  Memory_store.get store.mem key
