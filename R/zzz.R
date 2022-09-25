.onLoad <- function(libname, pkgname) {
  # Default options --------------------------------------------------------
  op <- options()
  defaults <- list(
    rpresto.max.rows = 1000000L,
    rpresto.quiet = NA
  )
  toset <- !(names(defaults) %in% names(op))
  if (any(toset)) options(defaults[toset])

  invisible()
}
