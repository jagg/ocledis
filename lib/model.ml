open Base

type value =
  | Num of int32
  | String of string
  [@@deriving equal, hash, compare, sexp]

module Key = struct
  type t =
    | Num of int32
    | String of string  
  [@@deriving equal, hash, compare, sexp]
end

type update_op =
  | Set of Key.t * value
  | Delete of Key.t
  [@@deriving equal, hash, compare, sexp]
