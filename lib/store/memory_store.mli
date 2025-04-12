open Base
    
type t

val make : unit -> t
val update : t -> Store.update_op -> unit
val get : t -> Model.Key.t -> Model.value option
val iter : t -> (Store.update_op -> unit) -> unit
