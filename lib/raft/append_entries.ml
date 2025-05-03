open! Base

type t = {
  term : int;
  prev_log_index : int;
  prev_long_term : int;
  leader_commit_index : int;
  leader_id : State.Server_id.t;
  entries : State.Persistent_state.entry list;
}

type result = {
  success : bool;
  current_term : int;
}

let rec go_to_index log term =
  let (current : State.Persistent_state.entry option) = List.hd log in
  match current with
  | None -> []
  | Some entry -> if entry.term <= term then log
    else match List.tl log with
      | None -> []
      | Some lst -> go_to_index lst term

(** TODO
    If the commit index in the result is greater than the one in the input state
    we have to apply all those updates in the system state. This is not done here!
*)
let apply operation (state : State.t) =
  let from_index = go_to_index state.persistent.log operation.prev_log_index in
  let outdated = operation.term < state.persistent.current_term in
  let term_match = match List.hd from_index with
    | None -> false
    | Some entry -> entry.term = operation.prev_long_term
  in
  let new_log =
  if not outdated && term_match then
    List.append  state.persistent.log from_index
  else state.persistent.log in
  let last_index = Option.map ~f:(fun entry -> entry.index) (List.hd new_log) in
  let last_index = Option.value ~default:state.volatile.commit_index last_index in
  let new_state : State.t = {
    volatile = {
      state.volatile with commit_index = Int.min
                              last_index
                              operation.leader_commit_index
    };
    persistent = {
      state.persistent with
      log = new_log;
      current_term = Int.max
          state.persistent.current_term
          operation.term
    }}

  in
  let ops_count = new_state.volatile.commit_index - state.volatile.commit_index in
  let ops_to_apply = List.rev @@ List.take new_state.persistent.log ops_count in
  (new_state,
   ops_to_apply,
   {
      success = not outdated && term_match;
      current_term = new_state.persistent.current_term;
    }) 

