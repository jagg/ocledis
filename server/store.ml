open! Base
open Eio.Std

module Model = Kvlib.Model 

type command =
  | Set of Model.Key.t * Model.value * (string Promise.u)
  | Get of Model.Key.t * (Model.value option Promise.u)

type t = command Eio.Stream.t

let make sw pool =
  let stream = Eio.Stream.create 120 in
  let table = Kvlib.Storage.create () in
  let rec handler () =
    match Eio.Stream.take stream with
    | Set (key, value, resolver) ->
      let _ = match Kvlib.Storage.put table ~key ~value with
        | Ok _ ->
          Promise.resolve resolver "Commited";
        | Error e ->
          traceln "Error found storing value: %s" (Error.to_string_hum e);
          Promise.resolve resolver @@ Error.to_string_hum e;
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

let set store key value =
  let promise, resolver  = Promise.create () in
  Eio.Stream.add store (Set (key, value, resolver));
  (** We don't want to return until we know it's been committed *)
  Promise.await promise

let get store key =
  let promise, resolver  = Promise.create () in
  Eio.Stream.add store (Get (key, resolver));
  Promise.await promise
