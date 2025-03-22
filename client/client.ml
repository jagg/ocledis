open! Base
open Eio.Std

let run_client ~net ~addr =
  Switch.run @@ fun sw ->
                let flow = Eio.Net.connect ~sw net addr in
                let open Kvlib.Protocol in
                let from_server = Eio.Buf_read.of_flow flow ~max_size:4096 in
                Eio.Buf_write.with_flow flow @@ fun to_server ->
                                                let query = [ Set ("one", Num 2l); Get "one" ] in
                                                send_commands query to_server;
                                                let response = get_responses from_server in
                                                let response = Sexplib.Sexp.to_string_hum ([%sexp_of: response list] response) in
                                                traceln "[CLIENT] Response: %s" response
                                                

let client ~net ~addr =
  Switch.run @@ fun _ ->
                traceln "[CLIENT]: Starting";
                run_client ~net ~addr

let () =
  Eio_main.run @@ fun env ->
                  client ~net:(Eio.Stdenv.net env)
                  ~addr:(`Tcp (Eio.Net.Ipaddr.V4.loopback, 12342));;
