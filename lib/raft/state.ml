open! Base

module Server_id = struct
  module T = struct
    type t = Id of string
    [@@deriving compare, sexp]
  end
  include T
  include Comparator.Make(T)
end


module Persistent_state = struct

  type entry = {
    (** Index of the latest term in which the entry was added *)
    term : int;

    (** The index within the log, so that we don't have to count every time *)
    index : int;

    command : Kvlib.Model.update_op;
  }
  [@@deriving sexp]
  
  type t = {
    (** Index of the latest term the server has seen *)
    current_term : int;

    (** Candidate that received the vote in the current term *)
    voted_for : Server_id.t option;

    (** The log in reverse order, new entries go to the front *)
    log : entry list;

    (** The id of this server *)
    id : Server_id.t
  }
  [@@deriving sexp]
end

module Volatile_state = struct
  type mode =
    | Leader
    | Follower
    | Candidate
  [@@deriving sexp]

  type t = {
    mode : mode;
    
    (** Index of the highest log entry known to be commited *)
    commit_index : int;

    (** Index of the highest log entry applied to the state *)
    last_applied : int;

    (** Only for leaders *)
    (** Index of the next log entry to send to each server *)
    next_index : int Map.M(Server_id).t;

    (** Index of the highest log entry know to be replicated on each server *)
    match_index : int Map.M(Server_id).t;
  }
  [@@deriving sexp]
end

type t = {
  persistent : Persistent_state.t;
  volatile : Volatile_state.t;
}
[@@deriving sexp]

let last_log_index (state : t) =
  match List.hd state.persistent.log with
  | None -> 0
  | Some idx -> idx.index

let last_log_term (state : t) =
  match List.hd state.persistent.log with
  | None -> 0
  | Some idx -> idx.term


let inc_last_applied (state : t) =
  { state with
    volatile = {
      state.volatile with
      last_applied = 1 + state.volatile.last_applied;
    }
  }

let append_to_log (state : t) command =
  let entry : Persistent_state.entry =
    {
      term = state.persistent.current_term;
      index = 1 + last_log_index state;
      command;
    }
  in
  { state with
    persistent = {
      state.persistent with
      log = entry :: state.persistent.log;
    }
  }
