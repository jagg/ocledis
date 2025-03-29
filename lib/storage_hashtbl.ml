open! Base

type t = (Model.Key.t, Model.value) Hashtbl.t

let create () = Hashtbl.create (module Model.Key) 

let put table ~key ~value =
  Hashtbl.set table ~key ~data:value

let get table ~key =
  Hashtbl.find table key
