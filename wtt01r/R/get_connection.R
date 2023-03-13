# Some useful keyboard shortcuts for package authoring:
#
#   Install Package:           'Cmd + Shift + B'
#   Check Package:             'Cmd + Shift + E'
#   Test Package:              'Cmd + Shift + T'


library(DBI)
library(duckdb)

download_extension <- function(con) {
  name <- "wtt01"

  regex_major_version <- "^[0-9]+\\.[0-9]+\\.[0-9]+$"
  version <- "0.1.28"

  if (grepl(regex_major_version, version)) {
    env <- "prd"
  } else {
    env <- "dev"
  }

  bucket <- paste0("wtt-01-dist-", env)

  sys_info <- Sys.info()
  operating_system <- sys_info["sysname"]
  architecture <- sys_info["machine"]

  filename <- paste0(
    name,
    "-",
    version,
    "-",
    operating_system,
    "-",
    architecture,
    ".zip"
  )

  full_s3_path <- paste0(
    "https://",
    bucket,
    ".s3.us-west-2.amazonaws.com/extension/wtt01/",
    filename
  )

  print(full_s3_path)
  temp_dir <- tempdir()
  temp_file <- tempfile(tmpdir = temp_dir)
  download.file(full_s3_path, temp_file, method = "auto")
  unzip(temp_file, exdir = temp_dir)

  fp <- file.path(temp_dir, "wtt01.duckdb_extension")

  query <- paste0("INSTALL '", fp, "';")
  DBI::dbExecute(con, query)

  query <- "LOAD wtt01;"
  DBI::dbExecute(con, query)

  return(con)
}

get_version <- function() {
  return("0.1.28")
}

get_connection <- function(dbdir = ":memory:") {
  con <- DBI::dbConnect(
    duckdb::duckdb(config = list("allow_unsigned_extensions" = "true")),
    dbdir = dbdir
  )

  tryCatch(
    {
      query <- "LOAD 'wtt01';"
      DBI::dbExecute(con, query)

      return(con)
    },
    error = function(e) {
      return(download_extension(con))
    }
  )
}
