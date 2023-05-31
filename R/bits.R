#' Unsigned 32-bit integers.
#'
#' Storing frequently used, and sometimes difficult to encode, 32-bit integers.

Ox80000000 <- as.raw(c(rep(0, 31), 1))
Ox7fffffff <- as.raw(c(rep(1, 31), 0))
Ox30000000 <- intToBits(0x30000000)
unsigned_int <- 2^(0:31)
