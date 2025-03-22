open! Base

type value =
  | Num of int32
  | String of string
[@@deriving sexp]

type command =
  | Set of string * value
  | Get of string
[@@deriving sexp]

type response =
  | All_ok
  | Done of string * value 
  | Error of string
[@@deriving sexp]

val get_commands : Eio.Buf_read.t -> command list
val send_commands : command list -> Eio.Buf_write.t -> unit

val get_responses : Eio.Buf_read.t -> response list
val send_responses : response list -> Eio.Buf_write.t -> unit
