open! Base

type value =
  | Num of int32
  | String of string
[@@deriving sexp]

type command =
  | Set of string * value
  | Get of string
[@@deriving sexp]

let get_commands reader =
  let len = Bytes.of_string @@ Eio.Buf_read.take 4 reader in
  let len = Int.of_int32_exn (Stdlib.Bytes.get_int32_be len 0) in
  let msg = Eio.Buf_read.take len reader in
  let sexp = Parsexp.Single.parse_string_exn msg in
  ([%of_sexp: command list] sexp)

let send_commands commands writer =
  let sexp = [%sexp_of : command list] commands in
  let sexp = Sexplib.Sexp.to_string sexp in
  let len = String.length sexp in
  let buffer = Bytes.create (len + 4) in
  (** Not sure if there is a way to write into the socket without allocating buffers *)
  let _ = Encoding.push_str_exn 0 buffer sexp in
  Eio.Buf_write.bytes writer buffer
