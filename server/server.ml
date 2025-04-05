open! Base
open Eio.Std


let run_command store command = 
  let open Kvlib.Protocol in
  match command with
  | Set (key, value) ->
    let result = Store.set store key value in
    Save result
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

let () =
  Eio_main.run @@ fun env ->
  Switch.run ~name:"Server" @@ fun sw ->
  let dm = Eio.Stdenv.domain_mgr env in
  let pool = Eio.Executor_pool.create ~sw ~domain_count:2 dm in
  let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, 12342) in
  let net = Eio.Stdenv.net env in
  let socket = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 addr in
  let store = Store.make sw pool in
  traceln "[SERVER] Server ready!";
  Fiber.fork ~sw (fun () ->
      Eio.Net.run_server socket (handle_client store)
        ~additional_domains:(dm, 2)
        ~on_error:(traceln "Error found: %a" Fmt.exn))

