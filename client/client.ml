open! Base
open Eio.Std

let run_client ~net ~addr =
  Switch.run @@ fun sw ->
                let flow = Eio.Net.connect ~sw net addr in
                Eio.Buf_write.with_flow flow @@ fun to_server ->
                                                let buffer = Bytes.create 1024 in
                                                let _ = Encoding.push_str_exn 0 buffer "hi there" in
                                                Eio.Buf_write.bytes to_server buffer 

let client ~net ~addr =
  Switch.run @@ fun _ ->
                traceln "[CLIENT]: Starting";
                run_client ~net ~addr

let () =
  Eio_main.run @@ fun env ->
                  client ~net:(Eio.Stdenv.net env)
                  ~addr:(`Tcp (Eio.Net.Ipaddr.V4.loopback, 12342));;
