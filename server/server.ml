open! Core 
open Eio.Std

module Config = struct
  type t = {
    port : int;
    op_port : int;
    store : Kvlib.Store.config;
    cluster : Raft.cluster_config;
  }
  [@@deriving sexp]

  let default_config = {
    port = 12342;
    op_port = 12343;
    store = Kvlib.Store.default_config;
    cluster =
        {
          replicas = Map.singleton (module Raft.Server_id) (Id "one") ("127.0.0.1", 12343);
          quorum = 1;
        }
  }

  let load_cluster_config (file : string) : Raft.cluster_config =
    let sexp = Sexp.load_sexp file in
    Raft.cluster_config_of_sexp sexp

  let parse () = 

    let port = ref default_config.port in
    let op_port = ref default_config.op_port in
    let checkpoint_path = ref default_config.store.disk.checkpoint_path in
    let wal_path = ref default_config.store.disk.wal_path in
    let cluster_config = ref "./cluster.conf" in
    let mode = ref "l" in

    let speclist = [
      ("-p", Stdlib.Arg.Set_int port, "Input  port");
      ("-o", Stdlib.Arg.Set_int op_port, "Operations port");
      ("-c", Stdlib.Arg.Set_string checkpoint_path, "Input checkpoint path");
      ("-w", Stdlib.Arg.Set_string wal_path, "Input WAL path");
      ("-m", Stdlib.Arg.Set_string mode, "Mode: l or f");
      ("-f", Stdlib.Arg.Set_string cluster_config, "Path to cluster configuration")
    ] in

    let usage_msg = "Usage: server.exe -port 12342" in
    Stdlib.Arg.parse speclist (fun _ -> ()) usage_msg;
    {
      cluster = load_cluster_config !cluster_config;
      port = !port;
      op_port = !op_port;
      store = {
        mode = match !mode with
          | "l" -> Leader;
          | "f" -> Follower;
          | _ -> Leader; ;
        replica = default_config.store.replica;
        disk = {
          checkpoint_path = !checkpoint_path;
          wal_path = !wal_path;
        };
      };
    }
end

let run_command consensus sw net command = 
  let open Kvlib.Protocol in
  (* TODO Add new Raft operations here! *)
  match command with
  | Set (key, value) ->
    let result = Raft.store consensus sw net key value in
    Save result
  | Delete _key -> Error "Not implemented"
  | Get key ->
    match Raft.get consensus key with
    | None -> Error "The key was not present"
    | Some value -> Got (key, value)

let handle_client sw net consensus flow _addr =
  traceln "[SERVER] Got a connection";
  let open Kvlib.Protocol in
  let from_client = Eio.Buf_read.of_flow flow ~max_size:4096 in
  Eio.Buf_write.with_flow flow @@ fun to_client ->
  let query = get_commands from_client in
  let query_str = Sexplib.Sexp.to_string_hum ([%sexp_of: command list] query) in
  traceln "[SERVER] Query: %s" query_str;
  let response = List.map ~f:(fun cmd -> run_command consensus sw net cmd) query in
  send_responses response to_client

let () =
  let config = Config.parse () in
  Eio_main.run @@ fun env ->
  Switch.run ~name:"Server" @@ fun sw ->
  let config_str = Config.sexp_of_t config in
  traceln "Server Config:\n %s" @@ Sexp.to_string_hum config_str;
  let dm = Eio.Stdenv.domain_mgr env in
  let pool = Eio.Executor_pool.create ~sw ~domain_count:2 dm in
  let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, config.port) in
  let net = Eio.Stdenv.net env in
  let socket = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 addr in
  let consensus = Raft.make "id" sw env pool config.store in
  Raft.start consensus sw net config.op_port;
  traceln "[SERVER] Server ready!";
  Fiber.fork ~sw (fun () ->
      Eio.Net.run_server socket (handle_client sw net consensus)
        ~additional_domains:(dm, 2)
        ~on_error:(traceln "Error found: %a" Fmt.exn))

