open! Core
open! Core_bench

(** dune exec -- ./bench.exe -ascii -quota 0.25 *) 
let amount = 1000
let value_pairs =
  List.init amount ~f:(fun i -> ("key" ^ string_of_int i, Int32.of_int_exn i))

module type Storage = sig
  type t

  val make:  Kvlib.Store.config -> t
  val update : t -> Kvlib.Model.update_op -> unit Or_error.t
  val get : t -> Kvlib.Model.Key.t -> Kvlib.Model.value option
end

module Bench (S : Storage) = struct
  let run_bench () =
    let table = S.make Kvlib.Store.default_config in
    List.iter value_pairs
      ~f:(fun (key, value) ->
          let key = Kvlib.Model.Key.String key in
          let value = Kvlib.Model.Num value in
          let op = Kvlib.Model.Set (key, value) in
          Or_error.ok_exn (S.update table op);
          let _ = S.get table key in
          ()
        )
end 

module Bench_wal = Bench(Kvlib.Store)

let benchmarks =
  [ "WAL", Bench_wal.run_bench]

let () =
  List.map benchmarks ~f:(fun (name, test) ->
      Core_bench.Bench.Test.create ~name test)
  |> Core_bench.Bench.make_command
  |> Command_unix.run
