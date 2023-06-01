#' Format observation read from .yxdb Connection.
#'
#' Internal functions to convert raw data to various data types.
#'
#' R has native support for bool, byte, int32, double, character, and date only.
#' However, .yxdb files may contain a variety of additional field types. These
#' extractor functions are a brute force solution to coerce non-native data
#' types to something usable by R. In some cases (e.g., int64), data conversion
#' will occur by necessity.

bool_extractor <- function(start, field_len, buffer) {
  value <- buffer[start+1]
  if(value == 2) return(NA)
  return(value == 1)
}


byte_extractor <- function(start, field_len, buffer) {
  if(buffer[start+2] == 1) return(NA)
  return(buffer[start+1])
}


int16_extractor <- function(start, field_len, buffer) {
  if(buffer[start+3] == 1) return(NA)
  int <- as.integer(rawToBits(buffer[(start+1):(start+2)]))
  return(as.integer(sum((int[1:16] - int[16]) * 2^(0:15)) - int[16]))
}


int32_extractor <- function(start, field_len, buffer) {
  if(buffer[start+5] == 1) return(NA)
  return(packBits(rawToBits(buffer[(start+1):(start+4)]), 'integer'))
}


int64_extractor <- function(start, field_len, buffer) {
  if(buffer[start+9] == 1) return(NA)
  int <- as.integer(rawToBits(buffer[(start+1):(start+8)]))
  return(sum((int[1:64] - int[64]) * 2^(0:63)) - int[64])
}


fixed_decimal_extractor <- function(start, field_len, buffer) {
  if(buffer[start+field_len+1] == 1) return(NA)
  value = get_string(buffer, start, field_len, 1)
  return(as.numeric(value))
}


float_extractor <- function(start, field_len, buffer) {
  if(buffer[start+5] == 1) return(NA)
  int <- as.integer(rawToBits(buffer[(start+1):(start+4)]))
  s <- (-1) ^ int[32]
  if(sum(int[31:1]) == 0) return(0.0)
  if(sum(int[31:24]) == 8 & sum(int[23:1]) == 0) return(s * Inf)
  if(sum(int[31:24]) == 8 & sum(int[23:1]) > 0) return(NaN)
  ex <- sum(int[31:24] * 2 ^ (7:0)) - 127
  man <- sum(int[23:1] * 2 ^ (-1:-23)) + 1
  return(s*(2^ex)*man)
}


double_extractor <- function(start, field_len, buffer) {
  if(buffer[start+9] == 1) return(NA)
  return(packBits(rawToBits(buffer[(start+1):(start+8)]), "double"))
}


date_extractor <- function(start, field_len, buffer) {
  if(buffer[start+11] == 1) return(NA)
  return(parse_date(buffer, start, 10, "%Y-%m-%d"))
}


date_time_extractor <- function(start, field_len, buffer) {
  if(buffer[start+20] == 1) return(NA)
  return(parse_date(buffer, start, 19, "%Y-%m-%d %H:%M:%S"))
}


string_extractor <- function(start, field_len, buffer) {
  if(buffer[start+field_len+1] == 1) return(NA)
  return(get_string(buffer, start, field_len, 1))
}


wstring_extractor <- function(start, field_len, buffer) {
  if(buffer[start+(field_len*2)+1] == 1) return(NA)
  return(get_string(buffer, start, field_len, 2))
}


v_string_extractor <- function(start, field_len, buffer) {
  blob <- parse_blob(buffer, start)
  if(is.null(blob)) return(NA)
  return(rawToChar(blob))
}


v_wstring_extractor <- function(start, field_len, buffer) {
  blob <- parse_blob(buffer, start)
  if(is.null(blob)) return(NA)
  blob <- blob[seq(1, length(blob), 2)]
  return(rawToChar(blob))
}


blob_extractor <- function(start, field_len, buffer) {
  return(parse_blob(buffer, start))
}


parse_date <- function(buffer, start, length, str) {
  value <- rawToChar(buffer[(start+1):(start+length)])
  return(as.Date(value, "%Y-%m-%d"))
}


get_string <- function(buffer, start, field_len, char_size) {
  end <- get_end_of_string_pos(buffer, start, field_len, char_size)
  if(char_size == 1) {
    return(rawToChar(buffer[(start+1):end]))
  } else {
    return(rawToChar(buffer[(start+1):end])) # PROBABLY NEED TO DO EVERY OTHER CHAR
  }
}


get_end_of_string_pos <- function(buffer, start, field_len, char_size) {
  field_to <- start + (field_len * char_size)
  str_len = 0
  i <- start
  while(i < field_to) {
    if((buffer[i+1] == 0) & (buffer[i + (char_size-1) +1] == 0)) break
  }
  str_len <- str_len + 1
  i <- i + char_size
  return(start + (str_len * char_size))
}


parse_blob <- function(buffer, start) {
  fixed_portion_raw <- rawToBits(buffer[(start+1):(start+4)])
  fixed_portion <- sum(as.integer(fixed_portion_raw) * unsigned_int)
  if(fixed_portion == 0) return(raw(0))
  if(fixed_portion == 1) return(NULL)

  if(is_tiny(fixed_portion_raw)) return(get_tiny_blob(start, buffer))

  block_start <- start + sum(as.integer(fixed_portion_raw & Ox7fffffff) * unsigned_int)

  block_first_byte <- buffer[block_start+1]
  if(is_small_block(block_first_byte)) {
    return(get_small_blob(buffer, block_start))
  } else {
    return(get_normal_blob(buffer, block_start))
  }
}


is_tiny <- function(fixed_portion) {
  lhs <- sum(as.integer(fixed_portion & Ox80000000) * unsigned_int)
  rhs <- sum(as.integer(fixed_portion & Ox30000000) * unsigned_int)

  return((lhs == 0) & (rhs != 0))
}


get_tiny_blob <- function(start, buffer) {
  value <- sum(as.integer(rawToBits(buffer[(start+1):(start+4)])) * unsigned_int)
  length <- floor(value / (2^28))

  end <- start + length
  return(buffer[(start+1):end])
}


is_small_block <- function(value) {
  test <- sum(as.integer(intToBits(value) & intToBits(1)) * unsigned_int)
  return(test == 1)
}


get_small_blob <- function(buffer, block_start) {
  first_byte <- as.integer(buffer[block_start+1])
  blob_len <- floor(first_byte / (2^1))
  blob_start <- block_start + 1
  blob_end <- blob_start + blob_len
  return(buffer[(blob_start+1):blob_end])
}


get_normal_blob <- function(buffer, block_start) {
  blob_len <- sum(as.integer(rawToBits(buffer[(block_start+1):(block_start+4)])) * unsigned_int) / 2
  blob_start <- block_start + 4
  blob_end <- blob_start + blob_len
  return(buffer[(blob_start+1):blob_end])
}
