open! Base

type t

val make : Eio.Switch.t -> t
val set : t -> Kvlib.Model.Key.t -> Kvlib.Model.value -> unit
val get : t -> Kvlib.Model.Key.t -> Kvlib.Model.value option 
