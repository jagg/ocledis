open! Base

type t = {
  term : int;
  candidate_id : State.Server_id.t;
  last_log_index : int;
  last_log_term : int;
}
[@@deriving sexp]

type result = {
  current_term : int;
  vote_granted : bool;
}
[@@deriving sexp]

let emit (state : State.t) =
  {
    term = state.persistent.current_term;
    candidate_id = state.persistent.id;
    last_log_index = State.last_log_index state;
    last_log_term = State.last_log_term state;
  }

let apply operation (state : State.t) =
  let outdated = operation.term < state.persistent.current_term in
  let voted_someone_else = match state.persistent.voted_for with
    | None -> false
    | Some id -> State.Server_id.compare id operation.candidate_id = 0
  in
  let candidate_up_to_date =
    if operation.last_log_term = (State.last_log_term state) then
      operation.last_log_index >= (State.last_log_index state)
    else
      operation.last_log_term > (State.last_log_term state)
  in
  let current_term = Int.max operation.last_log_term state.persistent.current_term in
  if not outdated && candidate_up_to_date && not voted_someone_else then
    (
      {
        state with 
        persistent = { state.persistent with
                       voted_for = Some operation.candidate_id;
                       current_term;
                     };
      },
      {
        current_term;
        vote_granted = true;
      }
    )
  else
    (state, {
        current_term;
        vote_granted = false;
      })


let%expect_test "test_apply_happy_path" =
  let log : State.Persistent_state.entry list = [] in
  let (persistent : State.Persistent_state.t) = {
    current_term = 0;
    voted_for = None;
    log = log;
    id = Id "follower";
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
    last_log_index = 1;
    last_log_term = 1;
    candidate_id = State.Server_id.Id "candidate";
  } in
  let result = apply operation state in 
  Core.print_s [%sexp (result : State.t * result)];
  [%expect {|
    (((persistent
       ((current_term 1) (voted_for ((Id candidate))) (log ())
        (id (Id follower))))
      (volatile
       ((mode Follower) (commit_index 0) (last_applied 0) (next_index ())
        (match_index ()))))
     ((current_term 1) (vote_granted true)))
    |}]


let%expect_test "test_apply_outdated_candidate" =
  let log : State.Persistent_state.entry list = [] in
  let (persistent : State.Persistent_state.t) = {
    current_term = 3;
    voted_for = None;
    log = log;
    id = Id "follower";
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
    last_log_index = 1;
    last_log_term = 1;
    candidate_id = State.Server_id.Id "candidate";
  } in
  let result = apply operation state in 
  Core.print_s [%sexp (result : State.t * result)];
  [%expect {|
    (((persistent ((current_term 3) (voted_for ()) (log ()) (id (Id follower))))
      (volatile
       ((mode Follower) (commit_index 3) (last_applied 3) (next_index ())
        (match_index ()))))
     ((current_term 3) (vote_granted false)))
    |}]


