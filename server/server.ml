open! Base
open Eio.Std

module Config = struct
  type t = {
    port : int;
    replica_port : int;
    checkpoint_path : string;
    wal_path : string
  }


  let default_config = {
    port = 12342;
    replica_port = 12343;
    checkpoint_path = "./checkpoint.bin";
    wal_path = "./wal.bin"
  }

  let parse () = 

    let port = ref default_config.port in
    let replica_port = ref default_config.replica_port in
    let checkpoint_path = ref default_config.checkpoint_path in
    let wal_path = ref default_config.wal_path in

    let speclist = [
      ("-p", Stdlib.Arg.Set_int port, "Input  port");
      ("-rp", Stdlib.Arg.Set_int replica_port, "Input replica port");
      ("-c", Stdlib.Arg.Set_string checkpoint_path, "Input checkpoint path");
      ("-w", Stdlib.Arg.Set_string wal_path, "Input WAL path")
    ] in

    let usage_msg = "Usage: server.exe -port 12342" in
    Stdlib.Arg.parse speclist (fun _ -> ()) usage_msg;
    {
      port = !port;
      replica_port = !replica_port;
      checkpoint_path = !checkpoint_path;
      wal_path = !wal_path
    }
end

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
  let config = Config.parse () in
  Eio_main.run @@ fun env ->
  Switch.run ~name:"Server" @@ fun sw ->
  let dm = Eio.Stdenv.domain_mgr env in
  let pool = Eio.Executor_pool.create ~sw ~domain_count:2 dm in
  let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, config.port) in
  let net = Eio.Stdenv.net env in
  let socket = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 addr in
  let store = Store.make sw pool in
  traceln "[SERVER] Server ready!";
  Fiber.fork ~sw (fun () ->
      Eio.Net.run_server socket (handle_client store)
        ~additional_domains:(dm, 2)
        ~on_error:(traceln "Error found: %a" Fmt.exn))

