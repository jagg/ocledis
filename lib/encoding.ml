open Base

let encode_string pos buffer str =
  let len = String.length str in
  Stdlib.BytesLabels.blit_string ~src: str ~src_pos:0 ~dst: buffer ~dst_pos: pos ~len

let encode_int32 pos buffer int =
  Stdlib.Bytes.set_int32_be buffer pos int

let push_str_exn pos buffer str =
  let len = String.length str in
  let len32 = Int32.of_int_exn len in
  encode_int32 pos buffer len32;
  encode_string (pos + 4) buffer str;
  pos + 4 + len

let push_int32_exn pos buffer num =
  encode_int32 pos buffer num;
  Int.of_int32_exn num + 4
