open! Base
open Eio.Std

type 'v command =
  | Set of string * 'v
  | Get of string * ('v option Promise.u)

type 'v t = 'v command Eio.Stream.t

let make sw =
  let stream = Eio.Stream.create 120 in
  let table = Hashtbl.create (module String) in
  let rec handler () =
    match Eio.Stream.take stream with
    | Set (key, value) ->
       let _ = Hashtbl.add table ~key ~data:value in
       handler ()
    | Get (key, resolver) ->
       let value = Hashtbl.find table key in
       Promise.resolve resolver value;
       handler ()
  in
  Fiber.fork ~sw handler;
  stream

let set store key value =
  Eio.Stream.add store (Set (key, value))

let get store key =
  let promise, resolver  = Promise.create () in
  Eio.Stream.add store (Get (key, resolver));
  Promise.await promise
