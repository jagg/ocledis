open! Base

type t = {
  term : int;
  prev_log_index : int;
  prev_log_term : int;
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

let term_at_index log index =
  match List.hd (go_to_index log index) with
  | None -> 0
  | Some entry -> entry.term

let emit (state : State.t) (id : State.Server_id.t) =
  let follower_next_idx = Option.value ~default:(State.last_log_index state + 1) @@
    Map.find state.volatile.next_index id
  in
  let entries = if (State.last_log_index state) >= follower_next_idx then
    List.take_while state.persistent.log
      ~f:(fun entry -> entry.index >= follower_next_idx)
      else []
  in
  {
    term = state.persistent.current_term;
    prev_log_index = follower_next_idx - 1;
    prev_log_term = term_at_index state.persistent.log @@ follower_next_idx - 1;
    leader_commit_index = state.volatile.commit_index;
    leader_id = state.persistent.id;
    entries;
  }

let emit_all (state : State.t) =
  Map.fold state.volatile.next_index ~init:[]
    ~f:(fun ~key ~data:_ acc -> (emit state key)::acc)

(** TODO
    If the commit index in the result is greater than the one in the input state
    we have to apply all those updates in the system state. This is not done here!
*)
let apply operation (state : State.t) =
  let from_index = go_to_index state.persistent.log operation.prev_log_index in
  let outdated = operation.term < state.persistent.current_term in
  let term_match = match List.hd from_index with
    | None -> true (* If the follower log is empty we can just fill it
                      in, as long as the leader is not outdated *)
    | Some entry -> entry.term = operation.prev_log_term
  in
  let new_log =
  if not outdated && term_match then
    List.append  operation.entries from_index
  else state.persistent.log in
  let last_index = Option.map ~f:(fun entry -> entry.index) (List.hd new_log) in
  let last_index = Option.value ~default:state.volatile.commit_index last_index in
  let new_vote = if operation.term > state.persistent.current_term then
      None
    else
      state.persistent.voted_for
  in
  let new_state : State.t = {
    volatile = {
      state.volatile with commit_index = Int.min
                              last_index
                              operation.leader_commit_index
    };
    persistent = {
      state.persistent with
      log = new_log;
      voted_for = new_vote;
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

let%expect_test "go_to_index" =
  let log : State.Persistent_state.entry list = [
    { term = 2; index = 4; command = Delete (String "test") };
    { term = 1; index = 3; command = Delete (String "test") };
    { term = 1; index = 2; command = Delete (String "test") };
    { term = 0; index = 1; command = Delete (String "test") };
    { term = 0; index = 0; command = Delete (String "test") };
  ] in
  let result = go_to_index log 2 in
  Core.print_s [%sexp (result : State.Persistent_state.entry list)];
  [%expect {|
    (((term 1) (index 2) (command (Delete (String test))))
     ((term 0) (index 1) (command (Delete (String test))))
     ((term 0) (index 0) (command (Delete (String test)))))
    |}]


let%expect_test "test_emit_happy_path" =
  let log : State.Persistent_state.entry list = [
    { term = 3; index = 3; command = Delete (String "test") };
    { term = 2; index = 2; command = Delete (String "test") };
    { term = 1; index = 1; command = Delete (String "test") };
  ] in
  let (persistent : State.Persistent_state.t) = {
    current_term = 3;
    voted_for = None;
    log = log;
    id = Id "leader";
  } in
  let (volatile : State.Volatile_state.t) = {
    mode = Leader;
    commit_index = 2;
    last_applied = 2;
    next_index = Map.singleton (module State.Server_id) (Id "one") 1;
    match_index = Map.singleton (module State.Server_id) (Id "one") 0;
  } in
  let (state : State.t) = {
    persistent;
    volatile;
  } in
  let result = emit state (Id "one") in 
  Core.print_s [%sexp (result : t)];
  [%expect {|
    ((term 3) (prev_log_index 0) (prev_log_term 0) (leader_commit_index 2)
     (leader_id (Id leader))
     (entries
      (((term 3) (index 3) (command (Delete (String test))))
       ((term 2) (index 2) (command (Delete (String test))))
       ((term 1) (index 1) (command (Delete (String test)))))))
    |}]

let%expect_test "test_emit_heartbeat" =
  let log : State.Persistent_state.entry list = [
    { term = 3; index = 3; command = Delete (String "test") };
    { term = 2; index = 2; command = Delete (String "test") };
    { term = 1; index = 1; command = Delete (String "test") };
  ] in
  let (persistent : State.Persistent_state.t) = {
    current_term = 3;
    voted_for = None;
    log = log;
    id = Id "leader";
  } in
  let (volatile : State.Volatile_state.t) = {
    mode = Leader;
    commit_index = 3;
    last_applied = 3;
    next_index = Map.singleton (module State.Server_id) (Id "one") 4;
    match_index = Map.singleton (module State.Server_id) (Id "one") 3;
  } in
  let (state : State.t) = {
    persistent;
    volatile;
  } in
  let result = emit state (Id "one") in 
  Core.print_s [%sexp (result : t)];
  [%expect {|
    ((term 3) (prev_log_index 3) (prev_log_term 3) (leader_commit_index 3)
     (leader_id (Id leader)) (entries ()))
    |}]

let%expect_test "test_apply_happy_path" =
  let leader_log : State.Persistent_state.entry list = [
    { term = 3; index = 4; command = Delete (String "test") };
    { term = 2; index = 3; command = Delete (String "test") };
  ] in
  let log : State.Persistent_state.entry list = [
    { term = 2; index = 2; command = Delete (String "test") };
    { term = 1; index = 1; command = Delete (String "test") };
    { term = 1; index = 0; command = Delete (String "test") };
  ] in
  let (persistent : State.Persistent_state.t) = {
    current_term = 2;
    voted_for = None;
    log = log;
    id = Id "id";
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
    prev_log_term = 2;
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
         (((term 3) (index 4) (command (Delete (String test))))
          ((term 2) (index 3) (command (Delete (String test))))
          ((term 2) (index 2) (command (Delete (String test))))
          ((term 1) (index 1) (command (Delete (String test))))
          ((term 1) (index 0) (command (Delete (String test))))))
        (id (Id id))))
      (volatile
       ((mode Follower) (commit_index 4) (last_applied 2) (next_index ())
        (match_index ()))))
     (((term 2) (index 3) (command (Delete (String test))))
      ((term 3) (index 4) (command (Delete (String test)))))
     ((success true) (current_term 2)))
    |}]




let%expect_test "test_apply_disagreement" =
  let leader_log : State.Persistent_state.entry list = [
    { term = 3; index = 4; command = Delete (String "test") };
    { term = 2; index = 3; command = Delete (String "test") };
  ] in
  let log : State.Persistent_state.entry list = [
    { term = 2; index = 4; command = Delete (String "test") };
    { term = 2; index = 3; command = Delete (String "test") };
    { term = 2; index = 2; command = Delete (String "test") };
    { term = 1; index = 1; command = Delete (String "test") };
    { term = 1; index = 0; command = Delete (String "test") };
  ] in
  let (persistent : State.Persistent_state.t) = {
    current_term = 2;
    voted_for = None;
    log = log;
    id = Id "id";
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
    term = 3;
    prev_log_index = 2;
    prev_log_term = 2;
    leader_commit_index = 4;
    leader_id = State.Server_id.Id "one";
    entries = leader_log;
  } in
  let result = apply operation state in 
  Core.print_s [%sexp (result : State.t * State.Persistent_state.entry list * result
)];
  [%expect {|
    (((persistent
       ((current_term 3) (voted_for ())
        (log
         (((term 3) (index 4) (command (Delete (String test))))
          ((term 2) (index 3) (command (Delete (String test))))
          ((term 2) (index 2) (command (Delete (String test))))
          ((term 1) (index 1) (command (Delete (String test))))
          ((term 1) (index 0) (command (Delete (String test))))))
        (id (Id id))))
      (volatile
       ((mode Follower) (commit_index 4) (last_applied 2) (next_index ())
        (match_index ()))))
     (((term 2) (index 3) (command (Delete (String test))))
      ((term 3) (index 4) (command (Delete (String test)))))
     ((success true) (current_term 3)))
    |}]



let%expect_test "test_apply_ignore_old_leader" =
  let leader_log : State.Persistent_state.entry list = [
    { term = 2; index = 4; command = Delete (String "test") };
    { term = 2; index = 3; command = Delete (String "test") };
  ] in
  let log : State.Persistent_state.entry list = [
    { term = 3; index = 4; command = Delete (String "test") };
    { term = 3; index = 3; command = Delete (String "test") };
    { term = 2; index = 2; command = Delete (String "test") };
    { term = 1; index = 1; command = Delete (String "test") };
    { term = 1; index = 0; command = Delete (String "test") };
  ] in
  let (persistent : State.Persistent_state.t) = {
    current_term = 3;
    voted_for = None;
    log = log;
    id = Id "id";
  } in
  let (volatile : State.Volatile_state.t) = {
    mode = Follower;
    commit_index = 3;
    last_applied = 3;
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
    prev_log_term = 1;
    leader_commit_index = 3;
    leader_id = State.Server_id.Id "one";
    entries = leader_log;
  } in
  let result = apply operation state in 
  Core.print_s [%sexp (result : State.t * State.Persistent_state.entry list * result
)];
  [%expect {|
    (((persistent
       ((current_term 3) (voted_for ())
        (log
         (((term 3) (index 4) (command (Delete (String test))))
          ((term 3) (index 3) (command (Delete (String test))))
          ((term 2) (index 2) (command (Delete (String test))))
          ((term 1) (index 1) (command (Delete (String test))))
          ((term 1) (index 0) (command (Delete (String test))))))
        (id (Id id))))
      (volatile
       ((mode Follower) (commit_index 3) (last_applied 3) (next_index ())
        (match_index ()))))
     () ((success false) (current_term 3)))
    |}]



let%expect_test "test_first_call" =
  let leader_log : State.Persistent_state.entry list = [
    { term = 1; index = 1; command = Delete (String "test") };
    { term = 1; index = 0; command = Delete (String "test") };
  ] in
  let log : State.Persistent_state.entry list = [] in
  let (persistent : State.Persistent_state.t) = {
    current_term = 0;
    voted_for = None;
    log = log;
    id = Id "id";
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
  let operation = {
    term = 1;
    prev_log_index = -1;
    prev_log_term = 0;
    leader_commit_index = 1;
    leader_id = State.Server_id.Id "one";
    entries = leader_log;
  } in
  let result = apply operation state in 
  Core.print_s [%sexp (result : State.t * State.Persistent_state.entry list * result
)];
  [%expect {|
    (((persistent
       ((current_term 1) (voted_for ())
        (log
         (((term 1) (index 1) (command (Delete (String test))))
          ((term 1) (index 0) (command (Delete (String test))))))
        (id (Id id))))
      (volatile
       ((mode Follower) (commit_index 1) (last_applied 0) (next_index ())
        (match_index ()))))
     (((term 1) (index 1) (command (Delete (String test)))))
     ((success true) (current_term 1)))
    |}]







