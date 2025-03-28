open! Base

type 'v t = (string, 'v) Hashtbl.t

let create () = Hashtbl.create (module String) 

let put table ~key ~value =
  Hashtbl.set table ~key ~data:value

let get table ~key =
  Hashtbl.find table key
