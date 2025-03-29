open! Base

(** I should be able to generalize on the key using Functors *)
type t

val create :  unit -> t
val put : t -> key:Model.Key.t -> value:Model.value -> unit 
val get : t -> key:Model.Key.t -> Model.value option
