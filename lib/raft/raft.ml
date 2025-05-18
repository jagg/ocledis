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

type t =
  {
    mutable state : State.t;
    s_machine : State_machine.t;
    system_clock : float Time.clock_ty Std.r;
    cluster: cluster_config;
    mutable timer : Switch.t option; 
  }

let make id sw env net pool config =
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
    replicas = Map.singleton (module State.Server_id) (Id "one") ("127.0.0.1", 23);
    quorum = 2;
  } in
  {
    state;
    cluster;
    s_machine = State_machine.make sw net pool config;
    timer = None;
    system_clock = Eio.Stdenv.clock env;
  }

let trigger_election raft sw net =
  let msg = Request_vote.emit raft.state in
  let responses = List.map (Map.data raft.cluster.replicas) ~f:(fun (ip,port) ->
      Raft_api.send_vote_request sw net msg ip port)
  in
  let vote_count = List.fold responses ~init:0 ~f:(fun acc resp ->
      acc + (if resp.vote_granted then 1 else 0))
  in 
  let latest_term = List.fold responses ~init:0 ~f:(fun acc resp ->
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
  let msgs = Append_entries.emit_all raft.state in
  let responses = List.map msgs ~f:(fun msg ->
      match Map.find raft.cluster.replicas msg.destination_id with
      | Some (ip, port) -> Some (Raft_api.send_append sw net msg ip port);
      | None -> None;
    )
  in
  let latest_term = List.fold responses ~init:0 ~f:(fun acc resp ->
      let term = Option.map resp ~f:(fun r -> r.current_term) in
      Int.max acc (Option.value term ~default:0))
  in 
  let new_state : State.t = {
    persistent = { raft.state.persistent with
                   current_term = Int.max latest_term raft.state.persistent.current_term
                 };
    volatile = { raft.state.volatile with
                 mode = if latest_term > raft.state.persistent.current_term then Follower else Leader 
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
      let _ = match raft.state.volatile.mode with
        | Leader -> send_heartbeat raft sw net;
        | Follower -> trigger_election raft sw net;
        | Candidate -> trigger_election raft sw net;
      in
      reset_timer raft net
    )


let start raft net =
  Random.self_init ();
  reset_timer raft net


(* TODO Differentiate between Leader operations and Follower ones. This ones are for followers,
   but the leader needs to trigger an append when it receives a Set (k,v)
*)
let append_entries raft msg net =
  let (new_state, entries, result) = Append_entries.apply msg raft.state in
  raft.state <- new_state;
  if result.success then
    reset_timer raft net;
    List.iter entries ~f:(fun entry ->
        match entry.command with
        | Set (key, value) -> State_machine.set raft.s_machine key value;
        | Delete _key -> failwith "Error: Not implemented yet"
      );
  result

let process_vote_request raft msg =
  let (new_state, result) = Request_vote.apply msg raft.state in
  raft.state <- new_state;
  result
