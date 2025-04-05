open! Base

type t

type mode =
  | Leader
  | Replica


val make : ?mode:mode -> Eio.Switch.t -> Eio.Executor_pool.t -> t
val set : t -> Kvlib.Model.Key.t -> Kvlib.Model.value -> unit
val get : t -> Kvlib.Model.Key.t -> Kvlib.Model.value option 
