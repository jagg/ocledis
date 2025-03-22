open! Base
open Eio.Std

let handle_client client _addr =
  traceln "[SERVER] Got a connection";
  let reader = Eio.Buf_read.of_flow client ~max_size:2048 in
  let bytes = Bytes.of_string @@ Eio.Buf_read.take 4 reader in
  traceln "[SERVER] Got %d bytes" @@ Bytes.length bytes;
  let len = Int.of_int32_exn (Stdlib.Bytes.get_int32_be bytes 0) in
  traceln "[SERVER] Got a size: %d" len;
  let msg = Eio.Buf_read.take len reader in
  traceln "[SERVER] Got a msg: %s" msg


let run_server socket =
  Eio.Net.run_server socket handle_client
    ~on_error:(traceln "Error found: %a" Fmt.exn)

let server ~net ~addr =
  Switch.run ~name:"server" @@ fun sw ->
                             let socket = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 addr in
                             traceln "Server ready.";
                             Fiber.fork ~sw (fun () -> run_server socket)
  
let () =
  Eio_main.run @@ fun env ->
  server ~net:(Eio.Stdenv.net env)
         ~addr:(`Tcp (Eio.Net.Ipaddr.V4.loopback, 12342));;
