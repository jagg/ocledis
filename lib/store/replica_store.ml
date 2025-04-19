open! Base

type replica_config = {
  replicas : (string * int) list;
  quorum : int
}
[@@deriving sexp]

type t = {
  config : replica_config;
}
[@@deriving sexp]

let make config = {
  config
}

let to_command (op : Model.update_op) =
  match op with
  | Set (k,v) -> Protocol.Set (k,v)
  | Delete k -> Protocol.Delete k

let update replica op sw net =
  Or_error.try_with @@ fun () ->
  List.iter replica.config.replicas ~f:(fun (ip, port) ->
      let ipp = Unix.inet_addr_of_string ip in
      let ipv4 = Eio_unix.Net.Ipaddr.of_unix ipp in
      let addr = `Tcp (ipv4, port) in
      let flow = Eio.Net.connect ~sw net addr in
      let open Protocol in
      let command = to_command op in
      Eio.Buf_write.with_flow flow @@ fun to_server ->
      send_commands [command] to_server
    )
