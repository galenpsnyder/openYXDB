#' Collect next observation from .yxdb Connection.
#'
#' Checks if next observation is last and, if not, calls for next record to be read.
#'
#' @param yxdb .yxdb file connection as initialized by `get_header_and_field_info`.
#' @returns yxdb overwritten and staged for next observation.
#' @export
#' @examples
#' next_record()

next_record <- function(yxdb) {
  yxdb$current_record <- yxdb$current_record + 1
  if(yxdb$current_record > yxdb$total_records) {
    close(yxdb$stream)
    cat("All records read\n")
  }
  yxdb$record_buffer_index <- 0
  if(yxdb$has_var) {
    yxdb <- read_variable_record(yxdb)
  } else {
    yxdb <- read(yxdb)
  }
  yxdb
}


read_variable_record <- function(yxdb) {
  yxdb <- read(yxdb, yxdb$fixed_size+4)

  var_length <- yxdb$record_buffer[(yxdb$record_buffer_index-3):yxdb$record_buffer_index]
  # var_length <- packBits(rawToBits(var_length), type = "integer")
  var_length <- sum(as.integer(rawToBits(var_length)) * unsigned_int)
  if(yxdb$fixed_size + 4 + var_length > length(yxdb$record_buffer)) {
    new_length <- (yxdb$fixed_size + 4 + var_length) * 2
    new_buffer <- raw(length = new_length)
    new_buffer[1:(yxdb$fixed_size+4)] <- yxdb$record_buffer[1:(yxdb$fixed_size+4)]
    yxdb$record_buffer <- new_buffer
  }
  yxdb <- read(yxdb, size = var_length)
  yxdb
}


read <- function(yxdb, size) {
  while(size > 0) {
    if(yxdb$lzf_out_size == 0) {
      yxdb <- read_next_lzf_block(yxdb)
    }

    while(size + yxdb$lzf_out_index > yxdb$lzf_out_size) {
      remaining_lzf <- yxdb$lzf_out_size - yxdb$lzf_out_index
      yxdb$record_buffer[(yxdb$record_buffer_index+1):(yxdb$record_buffer_index+remaining_lzf)] <- yxdb$lzf_out[(yxdb$lzf_out_index+1):(yxdb$lzf_out_index+remaining_lzf)]
      yxdb$record_buffer_index <- yxdb$record_buffer_index + remaining_lzf
      size <- size - remaining_lzf
      yxdb <- read_next_lzf_block(yxdb)
      yxdb$lzf_out_index <- 0
    }

    len_to_copy <- min(yxdb$lzf_out_size, size)
    yxdb$record_buffer[(yxdb$record_buffer_index+1):(yxdb$record_buffer_index+len_to_copy)] <- yxdb$lzf_out[(yxdb$lzf_out_index+1):(yxdb$lzf_out_index+len_to_copy)]
    yxdb$lzf_out_index <- yxdb$lzf_out_index + len_to_copy
    yxdb$record_buffer_index <- yxdb$record_buffer_index + len_to_copy
    size <- size - len_to_copy
  }

  yxdb
}


read_next_lzf_block <- function(yxdb) {
  yxdb <- read_lzf_block_length(yxdb)
  # lzf_block_length <- packBits(rawToBits(yxdb$lzf_length_buffer), type = "integer")
  # checkbit <- bin_and(lzf_block_length, 0x80000000)
  lzf_block_length <- rawToBits(yxdb$lzf_length_buffer)
  checkbit <- sum(as.integer(lzf_block_length & Ox80000000) * unsigned_int)
  if(checkbit > 0) {
    # lzf_block_length <- bin_and(lzf_block_length, 0x7fffffff)
    lzf_block_length <- sum(as.integer(lzf_block_length & Ox7fffffff) * unsigned_int)
    yxdb$lzf_out[1:(lzf_block_length)] <- readBin(yxdb$stream, raw(), lzf_block_length)
    yxdb$lzf_out_size <- lzf_block_length
  } else {
    lzf_block_length <- sum(as.integer(lzf_block_length) * unsigned_int)
    yxdb$lzf_in[1:(lzf_block_length)] <- readBin(yxdb$stream, raw(), lzf_block_length)
    yxdb <- decompress(yxdb, lzf_block_length)
    yxdb$lzf_out_size <- yxdb$oidx
  }
  yxdb
}



read_lzf_block_length <- function(yxdb) {
  yxdb$lzf_length_buffer[] <- readBin(yxdb$stream, raw(), 4)
  yxdb
}



decompress <- function(yxdb, length) {
  in_len <- length
  yxdb$iidx <- 0
  yxdb$oidx <- 0
  ox1f <- packBits(intToBits(0x1f))

  if (in_len == 0){
    return(0)
  }

  while(yxdb$iidx < in_len){
    ctrl <- yxdb$lzf_in[yxdb$iidx+1]
    yxdb$iidx <- yxdb$iidx + 1

    if(ctrl < 32) {
      # copy_byte_sequence(ctrl)
      len <- as.integer(ctrl) + 1
      if((yxdb$oidx + len) > length(yxdb$lzf_out)) stop("copy byte error")
      yxdb$lzf_out[(yxdb$oidx+1):(yxdb$oidx + len)] <- yxdb$lzf_in[(yxdb$iidx+1):(yxdb$iidx + len)]
      yxdb$oidx <- yxdb$oidx + len
      yxdb$iidx <- yxdb$iidx + len
    }
    else {
      # expand_repeated_bytes(ctrl)
      len <- floor(as.integer(ctrl) / (2^5))
      ref <- yxdb$oidx - (as.integer(ctrl & ox1f)[1] * (2 ^ 8)) - 1
      if(len == 7) {
        len <- len + as.integer(yxdb$lzf_in[yxdb$iidx+1])
        yxdb$iidx <- yxdb$iidx + 1
      }
      if((yxdb$oidx + len + 2) > length(yxdb$lzf_out)) stop("expand byte error")
      ref <- ref - as.integer(yxdb$lzf_in[yxdb$iidx+1])
      yxdb$iidx <- yxdb$iidx + 1
      len <- len + 2
      while(len > 0) {
        size <- min(yxdb$oidx - ref, len)
        yxdb$lzf_out[(yxdb$oidx+1):(yxdb$oidx+size)] <- yxdb$lzf_out[(ref+1):(ref+size)]
        yxdb$oidx <- yxdb$oidx + size
        ref <- ref + size
        len <- len - size
      }
    }
  }
  yxdb
}
