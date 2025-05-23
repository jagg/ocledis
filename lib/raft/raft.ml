open! Base
open Eio

(*
Needs to include thre operations:
   1. Append
   2. Send Heartbeat
   3. Trigger election

The last two need to run on a timer, so they will need a switch from
Eio, and run in the background
*)

type cluster_config = {
  (** Id - IP - port*)
  replicas : (string * int) Map.M(State.Server_id).t;
  quorum : int
}
[@@deriving sexp]

module Server_id = State.Server_id
  
type t =
  {
    mutable state : State.t;
    s_machine : State_machine.t;
    system_clock : float Time.clock_ty Std.r;
    cluster: cluster_config;
    mutable timer : Switch.t option; 
  }

let make id sw env pool config =
  let (persistent : State.Persistent_state.t) = {
    current_term = 0;
    voted_for = None;
    log = [];
    id = Id id;
  } in
  let (volatile : State.Volatile_state.t) = {
    mode = Follower;
    commit_index = 0;
    last_applied = 0;
    next_index = Map.empty (module State.Server_id);
    match_index = Map.empty (module State.Server_id);
  } in
  let (state : State.t) = {
    persistent;
    volatile;
  } in
  (* TODO Remove hardcoded cluster config *)
  let cluster = {
    replicas = Map.singleton (module State.Server_id) (Id "one") ("127.0.0.1", 12343);
    quorum = 2;
  } in
  {
    state;
    cluster;
    s_machine = State_machine.make sw pool config;
    timer = None;
    system_clock = Eio.Stdenv.clock env;
  }

(* TODO Use Or_error to handle failed calls over the network, instead of letting it fail *)

let trigger_election raft sw net =
  traceln "Triggering election";
  let msg = Request_vote.emit raft.state in
  let responses = List.map (Map.data raft.cluster.replicas) ~f:(fun (ip,port) ->
      (* TODO run this in parallel! *)
      Raft_api.send_vote_request sw net msg ip port)
  in
  let ok_resp = List.filter_map responses ~f:(fun r -> match r with
      | Ok r -> Some r
      | Error e ->
        traceln "Trigger election error: %s" (Error.to_string_hum e);
        None)
  in
  let vote_count = List.fold ok_resp ~init:0 ~f:(fun acc resp ->
      acc + (if resp.vote_granted then 1 else 0))
  in 
  let latest_term = List.fold ok_resp ~init:0 ~f:(fun acc resp ->
      Int.max acc resp.current_term)
  in 
  let new_state : State.t = {
    persistent = { raft.state.persistent with
                   current_term = Int.max latest_term raft.state.persistent.current_term
                 };
    volatile = { raft.state.volatile with
                 mode = if vote_count >= List.length responses then Leader else Follower
               };
  } in
  raft.state <- new_state



let send_heartbeat raft sw net =
  traceln "Sending heartbeats";
  let msgs = Append_entries.emit_all raft.state in
  let responses = List.map msgs ~f:(fun msg ->
      (* TODO run this in parallel! *)
      match Map.find raft.cluster.replicas msg.destination_id with
      | Some (ip, port) ->
        traceln "Sending heartbeat to %s:%d" ip port;
        Some (Raft_api.send_append sw net msg ip port);
      | None ->
        traceln "Failed to send heartbeat, I don't know the id";
        None
    )
  in
  let latest_term = List.fold responses ~init:0 ~f:(fun acc resp ->
      let term = Option.map resp ~f:(fun r ->
          match r with
          | Ok r -> r.current_term
          | Error e ->
            traceln "Failed to send heartbeat: %s" (Error.to_string_hum e);
            0
        ) in
      Int.max acc (Option.value term ~default:0))
  in 
  let success_count = List.fold responses ~init:0 ~f:(fun acc resp ->
      let v = match resp with
        | Some r ->
          (match r with
           |  Ok r -> if r.success then 1 else 0
           |  Error _ -> 0) 
        | None -> 0
      in
      acc + v)
  in
  let commit = success_count >= raft.cluster.quorum in
  let new_state : State.t = {
    persistent = { raft.state.persistent with
                   current_term = Int.max latest_term raft.state.persistent.current_term
                 };
    volatile = { raft.state.volatile with
                 mode = if latest_term > raft.state.persistent.current_term then Follower else Leader;
                 commit_index = if commit then State.last_log_index raft.state else raft.state.volatile.commit_index;
               };
  } in
  raft.state <- new_state

 


let rec reset_timer raft net =
  Option.iter raft.timer ~f:(fun sw ->
      Eio.Switch.fail sw (Cancel.Cancelled Stdlib.Exit));
  Eio.Switch.run_protected ~name:"Clock" @@ fun sw ->
  raft.timer <- Some sw;
  Fiber.fork ~sw (fun () ->
      let secs = match raft.state.volatile.mode with
        | Leader -> 200.0 /. 1000.0;
        | Follower -> 1000.0 /. 1000.0;
        | Candidate -> 1000.0 /. 1000.0;
      in
      let wait_for = secs +. Random.float secs in
      traceln "Sleep for %.3f" wait_for;
      Eio.Time.sleep raft.system_clock wait_for;
      (* Trigger election and reset timer *)
      traceln "Waking up";
      let () = match raft.state.volatile.mode with
        | Leader ->
          traceln "I'm a leader!";
          send_heartbeat raft sw net;
        | Follower ->
          traceln "I'm a follower!";
          trigger_election raft sw net;
        | Candidate ->
          traceln "I'm a candidate!";
          trigger_election raft sw net;
      in
      reset_timer raft net
    )


let append_entries raft msg net =
  let (new_state, entries, result) = Append_entries.apply msg raft.state in
  raft.state <- new_state;
  if result.success then
    reset_timer raft net;
  List.iter entries ~f:(fun entry ->
      match entry.command with
      | Set (key, value) ->
        let _ = State_machine.set raft.s_machine key value in
        raft.state <- State.inc_last_applied raft.state
      | Delete _key -> failwith "Error: Not implemented yet"
    );
  result

let process_vote_request raft msg =
  let (new_state, result) = Request_vote.apply msg raft.state in
  raft.state <- new_state;
  result

let store raft sw net key value =
  match raft.state.volatile.mode with
  | Leader ->
    let open Kvlib.Model in 
    let result = State_machine.set raft.s_machine key value in
    raft.state <- State.append_to_log raft.state (Set (key, value));
    (* TODO Should only commit if we have quorum in the replicas *)
    send_heartbeat raft sw net;
    result;
  | Follower | Candidate -> failwith "Error: Not a leader"

let get raft key =
  State_machine.get raft.s_machine key

let handle_rpc raft net flow _addr =
  traceln "[SERVER-OP] Got a connection";
  let from_client = Eio.Buf_read.of_flow flow ~max_size:4096 in
  Eio.Buf_write.with_flow flow @@ fun to_client ->
  let rpc = Raft_api.get_rpc from_client in
  let rpc_str = Sexplib.Sexp.to_string_hum ([%sexp_of: Raft_api.rpc] rpc) in
  traceln "[SERVER] RPC: %s" rpc_str;
  let response = match rpc with
  | Append msg -> Raft_api.Append_response (append_entries raft msg net)
  | Request_vote msg -> Raft_api.Request_vote_response (process_vote_request raft msg)
  in
  Raft_api.send_response response to_client


let start raft sw net port =
  Random.self_init ();
  let addr = `Tcp (Eio.Net.Ipaddr.V4.loopback, port) in
  let socket = Eio.Net.listen net ~sw ~reuse_addr:true ~backlog:5 addr in
  Fiber.fork ~sw (fun () ->
      Eio.Net.run_server socket (handle_rpc raft net )
        ~on_error:(traceln "Error found: %a" Fmt.exn));
  traceln "Starting timer";
  reset_timer raft net


