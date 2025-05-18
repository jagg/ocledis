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

type t =
  {
    mutable state : State.t;
    s_machine : State_machine.t;
    system_clock : float Time.clock_ty Std.r;
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
  {
    state;
    s_machine = State_machine.make sw net pool config;
    timer = None;
    system_clock = Eio.Stdenv.clock env;
  }

let trigger_election raft =
  let msg = Request_vote.emit raft.state in
  msg

let send_heartbeat raft =
  let msgs = Append_entries.emit_all raft.state in
  msgs


let rec reset_timer raft =
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
        | Leader -> traceln "Send heartbeat";
        | Follower -> traceln "Trigger election";
        | Candidate -> traceln "Dispair";
      in
      reset_timer raft
    )


let start raft =
  Random.self_init ();
  reset_timer raft


let append_entries raft msg =
  let (new_state, entries, result) = Append_entries.apply msg raft.state in
  raft.state <- new_state;
  if result.success then
    reset_timer raft;
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
