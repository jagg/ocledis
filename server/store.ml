open! Base
open Eio.Std

module Model = Kvlib.Model 

type mode =
  | Leader
  | Replica

type command =
  | Set of Model.Key.t * Model.value
  | Get of Model.Key.t * (Model.value option Promise.u)

type t = command Eio.Stream.t

let make ?(mode=Leader) sw pool =
  let stream = Eio.Stream.create 120 in
  let table = Kvlib.Storage.create () in
  let rec handler () =
    match Eio.Stream.take stream with
    | Set (key, value) ->
      let _ = Kvlib.Storage.put table ~key ~value in
      let _ = match mode with
        | Leader -> ();
        | Replica -> ();
      in
      handler ()
    | Get (key, resolver) ->
      let value = Kvlib.Storage.get table ~key in
      Promise.resolve resolver value;
      handler ()
  in
  Fiber.fork ~sw (fun () ->
      Eio.Executor_pool.submit_exn pool
        ~weight:1.0
      handler);
  stream

;;

let set store key value =
  Eio.Stream.add store (Set (key, value))

let get store key =
  let promise, resolver  = Promise.create () in
  Eio.Stream.add store (Get (key, resolver));
  Promise.await promise
