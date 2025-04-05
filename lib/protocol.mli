open! Base

type command =
  | Set of Model.Key.t * Model.value
  | Get of Model.Key.t
[@@deriving sexp]

type response =
  | Save of string
  | Got of Model.Key.t * Model.value 
  | Error of string
[@@deriving sexp]

val get_commands : Eio.Buf_read.t -> command list
val send_commands : command list -> Eio.Buf_write.t -> unit

val get_responses : Eio.Buf_read.t -> response list
val send_responses : response list -> Eio.Buf_write.t -> unit
