open! Base

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
  let _ = Kvlib.Encoding.push_str_exn 0 buffer sexp in
  Eio.Buf_write.bytes writer buffer

let send_append sw net msg ip port =
  let ipp = Unix.inet_addr_of_string ip in
  let ipv4 = Eio_unix.Net.Ipaddr.of_unix ipp in
  let addr = `Tcp (ipv4, port) in
  let flow = Eio.Net.connect ~sw net addr in
  let from_server = Eio.Buf_read.of_flow flow ~max_size:4096 in
  Eio.Buf_write.with_flow flow @@ fun to_server ->
  send_sexp_list [%sexp_of: Append_entries.t] msg to_server;
  get_sexp_list [%of_sexp: Append_entries.result] from_server


let send_vote_request sw net msg ip port =
  let ipp = Unix.inet_addr_of_string ip in
  let ipv4 = Eio_unix.Net.Ipaddr.of_unix ipp in
  let addr = `Tcp (ipv4, port) in
  let flow = Eio.Net.connect ~sw net addr in
  let from_server = Eio.Buf_read.of_flow flow ~max_size:4096 in
  Eio.Buf_write.with_flow flow @@ fun to_server ->
  send_sexp_list [%sexp_of: Request_vote.t] msg to_server;
  get_sexp_list [%of_sexp: Request_vote.result] from_server
