open Base
 
type t

type disk_config = {
  wal_path : string;
  checkpoint_path : string;
}
[@@deriving sexp]

val start : disk_config -> Memory_store.t -> t
val process : t -> Model.update_op -> unit Or_error.t
val checkpoint : t -> Memory_store.t -> unit Or_error.t
