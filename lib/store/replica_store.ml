open! Base

type replica_config = {
  replicas : (string * int) list;
  quorum : int
}
[@@deriving sexp]

type t = {
  config : replica_config;
}
[@@deriving sexp]

let make config = {
  config
}

let update _replica _op =
  Ok ()
