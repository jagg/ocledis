open! Base

type t = (Model.Key.t, Model.value) Hashtbl.t

let create () = Hashtbl.create (module Model.Key) 

let put table ~key ~value =
  let _ = Hashtbl.set table ~key ~data:value in
  Or_error.return ()

let get table ~key =
  Hashtbl.find table key

let iter table ~f =
  Hashtbl.iteri table ~f
