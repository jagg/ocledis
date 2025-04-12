open! Base

type t = (Model.Key.t, Model.value) Hashtbl.t

let make () = Hashtbl.create (module Model.Key)

let update mem (op : Store.update_op) = 
  match op with
  | Set (key, data) -> Hashtbl.set mem ~key ~data
  | Delete key -> Hashtbl.remove mem key

let get mem key =
 Hashtbl.find mem key 

let iter mem (f:(Store.update_op -> unit)) =
  Hashtbl.iteri mem ~f:(fun ~key ~data ->
    f @@ Set (key, data))
