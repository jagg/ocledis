open! Base
open Eio.Std

let handle_client flow _addr =
  traceln "[SERVER] Got a connection";
  Eio.Flow.copy_string "Hi there" flow

let run_server socket =
  Eio.Net.run_server socket handle_client
    ~on_error:(traceln "Error found: %a" Fmt.exn)

let run_client ~net ~addr =
  Switch.run ~name:"client" @@ fun sw ->
                               traceln "[CLIENT]: Connecting to server";
                               let flow = Eio.Net.connect ~sw net addr in
                               traceln "Client: received %S" (Eio.Flow.read_all flow)
let main ~net ~addr =
  Switch.run ~name:"main" @@ fun sw ->
                             let server = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 addr in
                             Fiber.fork_daemon ~sw (fun () -> run_server server);
                             run_client ~net ~addr
  
let () =
  Eio_main.run @@ fun env ->
  main ~net:(Eio.Stdenv.net env)
       ~addr:(`Tcp (Eio.Net.Ipaddr.V4.loopback, 8080));;
