open! Base

type t

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

val make : config -> t
val update : t -> Model.update_op -> unit Or_error.t
val get : t -> Model.Key.t -> Model.value option
val default_config : config
