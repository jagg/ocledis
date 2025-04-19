open! Base

type t

val make : Eio.Switch.t ->
  [> [> `Generic ] Eio.Net.ty ] Eio.Resource.t
  -> Eio.Executor_pool.t
  -> Kvlib.Store.config
  -> t
val set : t -> Kvlib.Model.Key.t -> Kvlib.Model.value -> string
val get : t -> Kvlib.Model.Key.t -> Kvlib.Model.value option 
