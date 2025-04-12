open Base
 
type t

val start : Store.disk_config -> Memory_store.t -> t
val process : t -> Store.update_op -> unit Or_error.t
val checkpoint : t -> Memory_store.t -> unit Or_error.t
