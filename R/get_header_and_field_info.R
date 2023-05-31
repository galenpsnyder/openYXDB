#' Extract Header and Meta-Information from .yxdb Connection
#'
#' Reads header and collects meta-information from a .yxdb connection into a list.
#'
#' .yxdb files reserve their first bits for information about the file.
#' This includes the number of observations, field names, and field types.
#' `get_header_and_field_info` parses the first bits of a .yxdb connection
#' and returns a list containing field information, functions for extracting
#' data from fields of different formats, and initialized parameters for
#' reading and (most likely) decompressing the .yxdb file.
#'
#' @param path Path to .yxdb file.
#' @returns A list containing file meta-information and initialized parameters for file reading.
#' @export
#' @examples
#' get_header_and_field_info()

get_header_and_field_info <- function(path) {
  stream <- file(path, "rb")
  header <- readBin(stream, raw(), 512)
  file_type <- readBin(header[1:22], character())
  total_records <- readBin(header[105:109], integer())
  meta_info_size <- readBin(header[81:85], integer())
  meta_info <- readBin(stream, raw(), (meta_info_size*2)-2)
  readBin(stream, raw(), 2) # read next two to prep for real data reading
  meta_info_str <- rawToChar(meta_info[seq(1, length(meta_info), 2)])

  fields <- gsub("(.*<RecordInfo>\n\t)(.*)(</RecordInfo>.*)", "\\2", meta_info_str)
  fields <- strsplit(fields, "\n\t")[[1]]
  fields_list <- vector(mode = "list", length = length(fields))
  fixed_size <- 0
  has_var <- FALSE
  for(i in seq_along(fields)) {
    # raw string from header
    string <- fields[i]

    # parse components
    x <- gsub("(<)(.*)(/>)", "\\2", fields[i])

    if(grepl("Field name", x)) {
      name <- gsub("(.*Field name=\")(.*?)(\".*)", "\\2", x)
    } else {
      name <- NULL
    }

    if(grepl("size", x)) {
      size <- gsub("(.*size=\")(.*?)(\".*)", "\\2", x)
    } else {
      size <- NULL
    }

    if(grepl("type", x)) {
      type <- gsub("(.*type=\")(.*?)(\".*)", "\\2", x)
    } else {
      type <- NULL
    }

    if(grepl("scale", x)) {
      scale <- gsub("(.*scale\")(.*?)(\".*)", "\\2", x)
    } else {
      scale <- NULL
    }

    start <- fixed_size

    # add extractor and start values based on data parsed above
    if(type == 'Int16') {
      fixed_size <- fixed_size + 3
      extractor <- int16_extractor
      converter <- as.integer
    }
    if(type == 'Int32') {
      fixed_size <- fixed_size + 5
      extractor <- int32_extractor
      converter <- as.integer
    }
    if(type == 'Int64') {
      fixed_size <- fixed_size + 9
      extractor <- int64_extractor
      converter <- as.integer
    }
    if(type == 'Float') {
      fixed_size <- fixed_size + 5
      extractor <- float_extractor
      converter <- as.numeric
    }
    if(type == 'Double') {
      fixed_size <- fixed_size + 9
      extractor <- double_extractor
      converter <- as.numeric
    }
    if(type == 'FixedDecimal') {
      size <- field.size
      fixed_size <- fixed_size + size + 1
      extractor <- fixed_decimal_extractor
      converter <- as.numeric
    }
    if(type == 'String') {
      size = field.size
      fixed_size <- fixed_size + size + 1
      extractor <- string_extractor
      converter <- as.character
    }
    if(type == 'WString') {
      size <- field.size
      fixed_size <- fixed_size + (size * 2) + 1
      extractor <- wstring_extractor
      converter <- as.character
    }
    if(type == "V_String") {
      fixed_size <- fixed_size + 4
      has_var <- TRUE
      extractor <- v_string_extractor
      converter <- as.character
    }
    if(type == "V_WString") {
      fixed_size <- fixed_size + 4
      has_var <- TRUE
      extractor <- v_wstring_extractor
      converter <- as.character
    }
    if(type == "Date") {
      fixed_size <- fixed_size + 11
      extractor <- date_extractor
      converter <- as.Date.character
    }
    if(type == "DateTime") {
      fixed_size <- fixed_size + 2
      extractor <- date_time_extractor
      converter <- as.POSIXlt
    }
    if(type == "Bool") {
      fixed_size <- fixed_size + 1
      extractor <- bool_extractor
      converter <- as.logical
    }
    if(type == "Byte") {
      fixed_size <- fixed_size + 2
      extractor <- byte_extractor
      converter <- function(x) x
    }
    if(type == "Blob" | type == "SpatialObj") {
      fixed_size <- fixed_size + 4
      has_var <- TRUE
      extractor <- blob_extractor
      converter <- function(x) x
    }

    fields_list[[i]] <- list(
      name = name,
      type = type,
      size = size,
      scale = scale,
      start = start,
      extractor = extractor,
      converter = converter
    )
  }

  if(has_var) {
    record_buffer <- raw(length = fixed_size + 4 + 1000)
  } else {
    record_buffer <- raw(length = fixed_size)
  }


  lzf_buffer_size <- 262144
  lzf_in <- raw(lzf_buffer_size)
  lzf_out <- raw(lzf_buffer_size)
  in_bytes <- lzf_in
  out_bytes <- lzf_out
  iidx <- 0
  oidx <- 0
  lzf_length_buffer <- raw(4)
  current_record <- 0
  record_buffer_index <- 0
  lzf_out_size <- 0
  lzf_out_index <- 0

  out <- list(
    stream = stream,
    header = header,
    file_type = file_type,
    total_records = total_records,
    meta_info_size = meta_info_size,
    meta_info = meta_info,
    meta_info_str = meta_info_str,
    fields_list = fields_list,
    fixed_size = fixed_size,
    has_var = has_var,
    record_buffer = record_buffer,
    lzf_buffer_size = lzf_buffer_size,
    lzf_in = lzf_in,
    lzf_out = lzf_out,
    in_bytes = in_bytes,
    out_bytes = out_bytes,
    iidx = iidx,
    oidx = oidx,
    lzf_length_buffer = lzf_length_buffer,
    current_record = current_record,
    record_buffer_index = record_buffer_index,
    lzf_out_size = lzf_out_size,
    lzf_out_index = lzf_out_index
  )
}
