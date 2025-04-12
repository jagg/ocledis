open Base

type t

val make : Store.replica_config -> t
val update : t -> Store.update_op -> unit Or_error.t
