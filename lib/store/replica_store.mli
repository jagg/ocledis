open Base

type t

type replica_config = {
  replicas : (string * int) list;
  quorum : int
}
[@@deriving sexp]

val make : replica_config -> t
val update : t -> Model.update_op -> unit Or_error.t
