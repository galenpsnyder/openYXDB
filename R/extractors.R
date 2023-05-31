#' Format observation read from .yxdb Connection.
#'
#' Internal functions to convert raw data to various data types.

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
  return(packBits(rawToBits(buffer[(start+1):(start+2)]), 'integer'))
}


int32_extractor <- function(start, field_len, buffer) {
  if(buffer[start+5] == 1) return(NA)
  return(packBits(rawToBits(buffer[(start+1):(start+4)]), 'integer'))
}


int64_extractor <- function(start, field_len, buffer) {
  if(buffer[start+9] == 1) return(NA)
  return(packBits(rawToBits(buffer[(start+1):(start+8)]), 'integer'))
}


fixed_decimal_extractor <- function(start, field_len, buffer) {
  if(buffer[start+field_len+1] == 1) return(NA)
  value = get_string(buffer, start, field_len, 1) # DEFINE THIS FUNCTION
  return(as.numeric(value))
}


float_extractor <- function(start, field_len, buffer) {
  if(buffer[start+5] == 1) return(NA)
  return(packBits(rawToBits(buffer[(start+1):(start+4)]), "double")) # PROBABLY NOT CORRECT
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
  return(rawToChar(blob)) # NEED TO CREATE FUNCTION TO CONVERT TO UTF-8
}


v_wstring_extractor <- function(start, field_len, buffer) {
  blob <- parse_blob(buffer, start)
  if(is.null(blob)) return(NA)
  blob <- blob[seq(1, length(blob), 2)]
  return(rawToChar(blob)) # NEED TO CREATE FUNCTION TO CONVERT TO UTF-16
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
  # fixed_portion <- packBits(rawToBits(buffer[(start+1):(start+4)]), 'integer')
  fixed_portion_raw <- rawToBits(buffer[(start+1):(start+4)])
  fixed_portion <- sum(as.integer(fixed_portion_raw) * unsigned_int)
  if(fixed_portion == 0) return(raw(0))
  if(fixed_portion == 1) return(NULL)

  if(is_tiny(fixed_portion_raw)) return(get_tiny_blob(start, buffer))

  # block_start <- start + readBin(
  #   packBits(intToBits(fixed_portion)) & packBits(intToBits(0x7fffffff)),
  #   integer()
  # )
  block_start <- start + sum(as.integer(fixed_portion_raw & Ox7fffffff) * unsigned_int)

  block_first_byte <- buffer[block_start+1]
  if(is_small_block(block_first_byte)) {
    return(get_small_blob(buffer, block_start))
  } else {
    return(get_normal_blob(buffer, block_start))
  }
}


is_tiny <- function(fixed_portion) {
  # lhs <- bin_and(fixed_portion, 0x80000000)
  #
  # rhs <- bin_and(fixed_portion, 0x30000000)
  lhs <- sum(as.integer(fixed_portion & Ox80000000) * unsigned_int)
  rhs <- sum(as.integer(fixed_portion & Ox30000000) * unsigned_int)

  return((lhs == 0) & (rhs != 0))
}


get_tiny_blob <- function(start, buffer) {
  # value <- packBits(rawToBits(buffer[(start+1):(start+4)]), 'integer')
  value <- sum(as.integer(rawToBits(buffer[(start+1):(start+4)])) * unsigned_int)
  length <- floor(value / (2^28))

  end <- start + length
  return(buffer[(start+1):end])
}


is_small_block <- function(value) {
  # test <- bin_and(value, 1)
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
  # blob_len <- packBits(rawToBits(buffer[(block_start+1):(block_start+4)]), 'integer') / 2
  blob_len <- sum(as.integer(rawToBits(buffer[(block_start+1):(block_start+4)])) * unsigned_int) / 2
  blob_start <- block_start + 4
  blob_end <- blob_start + blob_len
  return(buffer[(blob_start+1):blob_end])
}
