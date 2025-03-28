open! Base

type 'v command =
  | Set of string * 'v
  | Get of string

