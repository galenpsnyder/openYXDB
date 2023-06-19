#' Read Data from .yxdb Connection
#'
#' Reads data from a .yxdb connection into a dataframe.
#' @param path Path to .yxdb file.
#' @param verbosity Numeric; print message after this many observations are read.
#' @returns A dataframe whose column types are determined by the .yxdb meta information.
#' @export
#' @examples
#' read_yxdb()

read_yxdb <- function(path, verbosity = 0) {
  if(!grepl(".yxdb$", path)) stop("`path` must point to a .yxdb file!")
  
  yxdb <- get_header_and_field_info(path)

  d_mat <- matrix(
    nrow = yxdb$total_records,
    ncol = length(yxdb$fields_list),
    dimnames = list(NULL, sapply(yxdb$fields_list, function(x) x$name))
  )

  for(i in seq_len(yxdb$total_records)) {
    yxdb <- next_record(yxdb)
    for(j in seq_along(yxdb$fields_list)) {
      ob <- yxdb$fields_list[[j]]$extractor(
        start = yxdb$fields_list[[j]]$start,
        field_len = yxdb$fields_list[[j]]$field_len,
        buffer = yxdb$record_buffer
      )
      if(length(ob) == 0 | is.null(ob)) {
        d_mat[i, j] <- NA
        err_attr <- paste0("unexpected NULL at observation ", i)
      } else if(length(ob) > 1){
        d_mat[i, j] <- paste(ob, collapse = " ")
        err_attr <- paste0("unexpected list-cell as observation ", i)
      } else {
        d_mat[i, j] <- ob
        err_attr <- "completed without error"
      }
    }
    if(verbosity > 0) {
      if(i %% verbosity == 0) cat(i, "rows read\n")
    }
  }

  close(yxdb$stream)

  d_df <- as.data.frame(d_mat)

  for(i in yxdb$fields_list) {
    d_df[[i$name]] <- i$converter(d_df[[i$name]])
  }

  d_df
}
