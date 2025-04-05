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

let get_sexp_list of_sexp reader =
  let len = Bytes.of_string @@ Eio.Buf_read.take 4 reader in
  let len = Int.of_int32_exn (Stdlib.Bytes.get_int32_be len 0) in
  let msg = Eio.Buf_read.take len reader in
  let sexp = Parsexp.Single.parse_string_exn msg in
  of_sexp sexp

let send_sexp_list of_sexp objs writer =
  let sexp = of_sexp objs in
  let sexp = Sexplib.Sexp.to_string sexp in
  let len = String.length sexp in
  let buffer = Bytes.create (len + 4) in
  (** Not sure if there is a way to write into the socket without allocating buffers *)
  let _ = Encoding.push_str_exn 0 buffer sexp in
  Eio.Buf_write.bytes writer buffer


let get_responses reader =
  get_sexp_list [%of_sexp: response list] reader

let send_responses responses writer =
  send_sexp_list [%sexp_of: response list] responses writer 

let get_commands reader =
  get_sexp_list [%of_sexp: command list]  reader

let send_commands commands writer =
  send_sexp_list [%sexp_of: command list] commands writer 
