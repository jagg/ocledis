open Base

type value =
  | Num of int32
  | String of string
[@@deriving sexp]

module Key = struct
  type t =
    | Num of int32
    | String of string  
  [@@deriving compare, sexp]

  let hash = function
    | Num n -> Int32.hash n
    | String s -> String.hash s

  let equal (a: t) (b: t) =
    compare a b = 0
end

type update_op =
  | Set of Key.t * value
  | Delete of Key.t
[@@deriving sexp]
