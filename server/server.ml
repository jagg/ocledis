open! Base
open Eio.Std


let run_command store command = 
  let open Kvlib.Protocol in
  match command with
  | Set (key, value) -> Store.set store key value; Stored_ok
  | Get key ->
     match Store.get store key with
     | None -> Error "The key was not present"
     | Some value -> Got (key, value)
  

  
let handle_client store flow _addr =
  traceln "[SERVER] Got a connection";
  let open Kvlib.Protocol in
  let from_client = Eio.Buf_read.of_flow flow ~max_size:4096 in
  Eio.Buf_write.with_flow flow @@ fun to_client ->
                                  let query = get_commands from_client in
                                  let query_str = Sexplib.Sexp.to_string_hum ([%sexp_of: command list] query) in
                                  traceln "[SERVER] Query: %s" query_str;
                                  let response = List.map ~f:(fun cmd -> run_command store cmd) query in
                                  send_responses response to_client

;;

let run_server socket store =
  Eio.Net.run_server socket (handle_client store)
    ~on_error:(traceln "Error found: %a" Fmt.exn)

;;

let server ~net ~addr =
  Switch.run ~name:"server" @@ fun sw ->
                               let store = Store.make sw in
                               traceln "Store ready";
                               let socket = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 addr in
                               traceln "Server ready.";
                               Fiber.fork ~sw (fun () -> run_server socket store)
;;

let () =
  Eio_main.run @@ fun env ->
                  server ~net:(Eio.Stdenv.net env)
                    ~addr:(`Tcp (Eio.Net.Ipaddr.V4.loopback, 12342))

;;
