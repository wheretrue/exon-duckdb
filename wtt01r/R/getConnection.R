# Hello, world!
#
# This is an example function named 'hello'
# which prints 'Hello, world!'.
#
# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Install Package:           'Cmd + Shift + B'
#   Check Package:             'Cmd + Shift + E'
#   Test Package:              'Cmd + Shift + T'



downloadExtension <- function(con) {
  library(httr)

  name <- "wtt01"
  env <- "prd"
  bucket <- paste0("wtt-01-dist-", env)
  version <- "0.1.21.dev1"

  sys_info <- Sys.info()
  operating_system <- sys_info["sysname"]
  architecture <- sys_info["machine"]

  filename = paste0(name, "-", version, "-", operating_system, "-", architecture, ".zip")

  full_s3_path = paste0(
    "https://",
    bucket,
    ".s3.us-west-2.amazonaws.com/extension/wtt01/",
    filename)

  print(full_s3_path)
  temp_dir <- tempdir()
  temp_file <- tempfile(tmpdir = temp_dir)
  download.file(full_s3_path, temp_file, method = "curl")
  unzip(temp_file, exdir = temp_dir)

  fp <- file.path(temp_dir, "wtt01.duckdb_extension")

  query = paste0("LOAD '", fp, "';");
  dbExecute(con, query)

  return(con)
}

getConnection <- function(dbdir = ":memory:") {
  library(DBI)
  library(duckdb)

  con <- dbConnect(
    duckdb::duckdb(config = list("allow_unsigned_extensions" = "true")),
    dbdir = dbdir)
  tryCatch({
    query <- "LOAD 'wtt01';"
    dbExecute(con, query)

    return(con)
  }, error = function(e) {
    return(downloadExtension(con))
  })
}
