open! Base

(** I should be able to generalize on the key using Functors *)
type 'v t

val create :  unit -> 'v t
val put : 'v t -> key:string -> value:'v -> unit 
val get : 'v t -> key:string -> 'v option
