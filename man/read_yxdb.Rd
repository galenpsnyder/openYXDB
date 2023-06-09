% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/read_yxdb.R
\name{read_yxdb}
\alias{read_yxdb}
\title{Read Data from .yxdb Connection}
\usage{
read_yxdb(path, verbosity = 0)
}
\arguments{
\item{path}{Path to .yxdb file.}

\item{verbosity}{Numeric; print message after this many observations are read.}
}
\value{
A dataframe whose column types are determined by the .yxdb meta information.
}
\description{
Reads data from a .yxdb connection into a dataframe.
}
\examples{
read_yxdb()
}
