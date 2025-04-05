open! Core
open! Core_bench

(** dune exec -- ./bench.exe -ascii -quota 0.25 *) 
let amount = 1000
let value_pairs =
  List.init amount ~f:(fun i -> ("key" ^ string_of_int i, Int32.of_int_exn i))

module type Storage = sig
  type t

  val create :  unit -> t
  val put : t -> key:Kvlib.Model.Key.t -> value:Kvlib.Model.value -> unit Or_error.t
  val get : t -> key:Kvlib.Model.Key.t -> Kvlib.Model.value option
end

module Bench (S : Storage) = struct
  let run_bench () =
    let table = S.create () in
    List.iter value_pairs
      ~f:(fun (key, value) ->
          let key = Kvlib.Model.Key.String key in
          let value = Kvlib.Model.Num value in
          Or_error.ok_exn @@ S.put table ~key ~value;
          let _ = S.get table ~key in
          ()
        )
end 

module Bench_hashtbl = Bench(Kvlib.Storage_hashtbl)
module Bench_wal = Bench(Kvlib.Write_ahead_log)

let benchmarks =
  [ "Hashtable", Bench_hashtbl.run_bench
  ; "WAL", Bench_wal.run_bench]

let () =
  List.map benchmarks ~f:(fun (name, test) ->
      Core_bench.Bench.Test.create ~name test)
  |> Core_bench.Bench.make_command
  |> Command_unix.run
