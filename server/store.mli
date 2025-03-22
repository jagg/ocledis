open! Base
open Eio.Std

type 'v t

val make : Eio.Switch.t -> 'v t
val set : 'v t -> string -> 'v -> unit
val get : 'v t -> string -> 'v option 
