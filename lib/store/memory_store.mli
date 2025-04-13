open Base
    
type t

val make : unit -> t
val update : t -> Model.update_op -> unit
val get : t -> Model.Key.t -> Model.value option
val iter : t -> (Model.update_op -> unit) -> unit
