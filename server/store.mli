open! Base

type t

val make : Eio.Switch.t -> Eio.Executor_pool.t -> t
val set : t -> Kvlib.Model.Key.t -> Kvlib.Model.value -> string
val get : t -> Kvlib.Model.Key.t -> Kvlib.Model.value option 
