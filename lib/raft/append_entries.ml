open! Base

type t = {
  term : int;
  prev_log_index : int;
  prev_long_term : int;
  leader_commit_index : int;
  leader_id : State.Server_id.t;
  entries : State.Persistent_state.entry list;
}
[@@deriving sexp]

type result = {
  success : bool;
  current_term : int;
}
[@@deriving sexp]

(** Assumes a log, sorted in reverse order by index, and finds the first entry
    with the same or smaller index
*)
let rec go_to_index log index =
  let (current : State.Persistent_state.entry option) = List.hd log in
  match current with
  | None -> []
  | Some entry -> if entry.index <= index then log
    else match List.tl log with
      | None -> []
      | Some lst -> go_to_index lst index

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
    List.append  operation.entries from_index
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

let%test_unit "basic" =
  [%test_eq: int list] (List.rev [3;2;1]) [1;2;3]


let%expect_test "go_to_index" =
  let log : State.Persistent_state.entry list = [
    { term = 2; index = 4; command = Get (String "test") };
    { term = 1; index = 3; command = Get (String "test") };
    { term = 1; index = 2; command = Get (String "test") };
    { term = 0; index = 1; command = Get (String "test") };
    { term = 0; index = 0; command = Get (String "test") };
  ] in
  let result = go_to_index log 2 in
  Core.print_s [%sexp (result : State.Persistent_state.entry list)];
  [%expect {|
    (((term 1) (index 2) (command (Get (String test))))
     ((term 0) (index 1) (command (Get (String test))))
     ((term 0) (index 0) (command (Get (String test)))))
    |}]


let%expect_test "test_apply_happy_path" =
  let leader_log : State.Persistent_state.entry list = [
    { term = 2; index = 4; command = Get (String "test") };
    { term = 1; index = 3; command = Get (String "test") };
  ] in
  let log : State.Persistent_state.entry list = [
    { term = 1; index = 2; command = Get (String "test") };
    { term = 0; index = 1; command = Get (String "test") };
    { term = 0; index = 0; command = Get (String "test") };
  ] in
  let (persistent : State.Persistent_state.t) = {
    current_term = 1;
    voted_for = None;
    log = log;
  } in
  let (volatile : State.Volatile_state.t) = {
    mode = Follower;
    commit_index = 2;
    last_applied = 2;
    next_index = Map.empty (module State.Server_id);
    match_index = Map.empty (module State.Server_id);
  } in
  let (state : State.t) = {
    persistent;
    volatile;
  } in
  let operation = {
    term = 2;
    prev_log_index = 2;
    prev_long_term = 1;
    leader_commit_index = 4;
    leader_id = State.Server_id.Id "one";
    entries = leader_log;
  } in
  let result = apply operation state in 
  Core.print_s [%sexp (result : State.t * State.Persistent_state.entry list * result
)];
  [%expect {|
    (((persistent
       ((current_term 2) (voted_for ())
        (log
         (((term 2) (index 4) (command (Get (String test))))
          ((term 1) (index 3) (command (Get (String test))))
          ((term 1) (index 2) (command (Get (String test))))
          ((term 0) (index 1) (command (Get (String test))))
          ((term 0) (index 0) (command (Get (String test))))))))
      (volatile
       ((mode Follower) (commit_index 4) (last_applied 2) (next_index ())
        (match_index ()))))
     (((term 1) (index 3) (command (Get (String test))))
      ((term 2) (index 4) (command (Get (String test)))))
     ((success true) (current_term 2)))
    |}]


let%expect_test "test_apply_disagreement" =
  let leader_log : State.Persistent_state.entry list = [
    { term = 2; index = 4; command = Get (String "test") };
    { term = 1; index = 3; command = Get (String "test") };
  ] in
  let log : State.Persistent_state.entry list = [
    { term = 1; index = 4; command = Get (String "test") };
    { term = 1; index = 3; command = Get (String "test") };
    { term = 1; index = 2; command = Get (String "test") };
    { term = 0; index = 1; command = Get (String "test") };
    { term = 0; index = 0; command = Get (String "test") };
  ] in
  let (persistent : State.Persistent_state.t) = {
    current_term = 1;
    voted_for = None;
    log = log;
  } in
  let (volatile : State.Volatile_state.t) = {
    mode = Follower;
    commit_index = 2;
    last_applied = 2;
    next_index = Map.empty (module State.Server_id);
    match_index = Map.empty (module State.Server_id);
  } in
  let (state : State.t) = {
    persistent;
    volatile;
  } in
  let operation = {
    term = 2;
    prev_log_index = 2;
    prev_long_term = 1;
    leader_commit_index = 4;
    leader_id = State.Server_id.Id "one";
    entries = leader_log;
  } in
  let result = apply operation state in 
  Core.print_s [%sexp (result : State.t * State.Persistent_state.entry list * result
)];
  [%expect {|
    (((persistent
       ((current_term 2) (voted_for ())
        (log
         (((term 2) (index 4) (command (Get (String test))))
          ((term 1) (index 3) (command (Get (String test))))
          ((term 1) (index 2) (command (Get (String test))))
          ((term 0) (index 1) (command (Get (String test))))
          ((term 0) (index 0) (command (Get (String test))))))))
      (volatile
       ((mode Follower) (commit_index 4) (last_applied 2) (next_index ())
        (match_index ()))))
     (((term 1) (index 3) (command (Get (String test))))
      ((term 2) (index 4) (command (Get (String test)))))
     ((success true) (current_term 2)))
    |}]



let%expect_test "test_apply_ignore_old_leader" =
  let leader_log : State.Persistent_state.entry list = [
    { term = 1; index = 4; command = Get (String "test") };
    { term = 1; index = 3; command = Get (String "test") };
  ] in
  let log : State.Persistent_state.entry list = [
    { term = 2; index = 4; command = Get (String "test") };
    { term = 2; index = 3; command = Get (String "test") };
    { term = 1; index = 2; command = Get (String "test") };
    { term = 0; index = 1; command = Get (String "test") };
    { term = 0; index = 0; command = Get (String "test") };
  ] in
  let (persistent : State.Persistent_state.t) = {
    current_term = 2;
    voted_for = None;
    log = log;
  } in
  let (volatile : State.Volatile_state.t) = {
    mode = Follower;
    commit_index = 2;
    last_applied = 2;
    next_index = Map.empty (module State.Server_id);
    match_index = Map.empty (module State.Server_id);
  } in
  let (state : State.t) = {
    persistent;
    volatile;
  } in
  let operation = {
    term = 1;
    prev_log_index = 2;
    prev_long_term = 1;
    leader_commit_index = 4;
    leader_id = State.Server_id.Id "one";
    entries = leader_log;
  } in
  let result = apply operation state in 
  Core.print_s [%sexp (result : State.t * State.Persistent_state.entry list * result
)];
  [%expect {|
    (((persistent
       ((current_term 2) (voted_for ())
        (log
         (((term 2) (index 4) (command (Get (String test))))
          ((term 2) (index 3) (command (Get (String test))))
          ((term 1) (index 2) (command (Get (String test))))
          ((term 0) (index 1) (command (Get (String test))))
          ((term 0) (index 0) (command (Get (String test))))))))
      (volatile
       ((mode Follower) (commit_index 4) (last_applied 2) (next_index ())
        (match_index ()))))
     (((term 2) (index 3) (command (Get (String test))))
      ((term 2) (index 4) (command (Get (String test)))))
     ((success false) (current_term 2)))
    |}]





