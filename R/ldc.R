# Requires:
# stringr
# httr
# jsonlite

#' Get an access token for the Landscape Data Commons API
#' @description A function for retrieving an access token for the Landscape Data
#' Commons API based on a username and password. The token is returned as a
#' specially-formatted list that can be used as the `token` argument in
#' [fetch_ldc()], [fetch_ldc_spatial()], and [fetch_ldc_ecosite()].
#' 
#' Some data in the LDC are fully available to the public and can be accessed
#' without an account, but some are restricted to accounts with specific
#' permissions and will not be returned without a valid token. The `username`
#' and `password` values must belong to an account which has already been
#' created at https://api.landscapedatacommons.org/login/.
#' 
#' Best practice for using this function is to use it to retrieve and store a
#' token in the working environment and then provide it to the fetching
#' functions in addition to the username and password. They will use the token
#' unless it has expired in which case they will retrieve a new token
#' automatically using the provided `username` and `password`. If a fetching
#' function informs you that it retrieved a new token, continuing to fetch will
#' retrieve a new token every time because the new tokens are internal to the
#' fetching process so consider rerunning the `get_ldc_token()` to get a new
#' token to reuse before continuing to fetch. A token is valid for one hour
#' after being retrieved.
#'
#' @param username Character string. The username tied to the Landscape Data Commons API account to use.
#' @param password Character string. The password to supply to the Landscape Data Commons API.
#' @returns A list structured for use as the `token` argument in the [fetch_ldc()] family of functions.
#' @examples
#' # To get a token to use for multiple data fetchings:
#' current_token <- get_ldc_token(username = "account email address",
#'                                password = "account password")
#' # To use the token to fetch the first 100 LPI records available to the account:
#' lpi_data <- fetch_ldc(data_type = "lpi",
#'                       token = current_token,
#'                       username = "account email address",
#'                       password = "account password",
#'                       take = 100)
#' @export

get_ldc_token <- function(username,
                          password) {
  if (is.character(username)) {
    if (length(username) > 1) {
      stop("Your username must be a single character string.")
    }
  } else {
    stop("Your username must be a single character string.")
  }
  if (is.character(password)) {
    if (length(password) > 1) {
      stop("Your password must be a single character string.")
    }
  } else {
    stop("Your password must be a single character string.")
  }
  
  # Attempt to get an authentication response
  authentication_response <- httr::POST(url = "https://oox5sjuicqhezohcpnbsesp32y0yrcbm.lambda-url.us-east-1.on.aws/",
                                        body = list(username = username, 
                                                    password = password),
                                        encode = "json")
  
  # What if there's an error????
  if (httr::http_error(authentication_response)) {
    stop(paste0("Retrieving authentication token from the API failed with status ",
                authentication_response$status_code))
  }
  
  output_raw_character <- rawToChar(authentication_response[["content"]])
  
  if (grepl(x = output_raw_character, pattern = "^Error")) {
    stop(output_raw_character)
  }
  
  
  output <- jsonlite::fromJSON(txt = rawToChar(authentication_response[["content"]]))[["AuthenticationResult"]]
  
  # We'll add an expiration time so we can check the need for a refreshed token
  # without making an API call that gets rejected.
  # This cuts 5 seconds off just as a bit of buffer.
  output[["expiration_time"]] <- Sys.time() + output[["ExpiresIn"]] - 5
  # Turns out this was overengineered, but keeping it for future reference.
  # output[["expiration_time"]] <- lubridate::as_date(lubridate::seconds(Sys.time()) + lubridate::seconds(output[["ExpiresIn"]] - 5))
  
  output
}

#' Fetching data from the Landscape Data Commons via API query
#' @description A function for making API calls to the Landscape Data Commons based on the table, key variable, and key variable values. It will return a table of records of the requested data type from the LDC in which the variable \code{key_type} contains only values found in \code{keys}. See the \href{https://api.landscapedatacommons.org/api-docs}{API documentation} to see which variables (i.e. \code{key_type} values) are valid for each data type.
#' 
#' There are additional functions to simplify querying by spatial location (\code{\link[=fetch_ldc_spatial]{fetch_ldc_spatial()}}) and by ecological site ID (\code{\link[=fetch_ldc_ecosite]{fetch_ldc_ecosite()}}).
#' @param keys Optional character vector. A character vector of all the values to search for in \code{key_type}. The returned data will consist only of records where \code{key_type} contained one of the key values, but there may be keys that return no records. If \code{NULL} then the entire table will be returned. Defaults to \code{NULL}.
#' @param key_type Optional character string. The name of the variable in the data to search for the values in \code{keys}. This must be the name of a variable that exists in the requested data type's table, e.g. \code{"PrimaryKey"} exists in all tables, but \code{"EcologicalSiteID"} is found only in some. If the function returns a status code of 500 as an error, this variable may not be found in the requested data type. If \code{NULL} then the entire table will be returned. Defaults to \code{NULL}.
#' @param data_type Character string. The type of data to query. Note that the variable specified as \code{key_type} must appear in the table corresponding to \code{data_type}. Valid values are: \code{'gap'}, \code{'header'}, \code{'height'}, \code{'lpi'}, \code{'soilstability'}, \code{'speciesinventory'}, \code{'indicators'}, \code{'species'}, \code{'dustdeposition'}, \code{'horizontalflux'}, and \code{'schema'}.
#' @param username Optional character string. The username to supply to the Landscape Data Commons API. Some data in the Landscape Data Commons are accessible only to users with appropriate credentials. You do not need to supply credentials, but an API request made without them may return fewer or no data. This argument will be ignored if \code{password} is \code{NULL}. Defaults to \code{NULL}.
#' @param password Optional character string. The password to supply to the Landscape Data Commons API.  Some data in the Landscape Data Commons are accessible only to users with appropriate credentials. You do not need to supply credentials, but an API request made without them may return fewer or no data. This argument will be ignored if \code{username} is \code{NULL}. Defaults to \code{NULL}.
#' @param key_chunk_size Numeric. The number of keys to send in a single query. Very long queries fail, so the keys may be chunked into smaller queries with the results of all the queries being combined into a single output. Defaults to \code{100}.
#' @param timeout Numeric. The number of seconds to wait for a nonresponse from the API before considering the query to have failed. Defaults to \code{300}.
#' @param take Optional numeric. The number of records to retrieve at a time. This is NOT the total number of records that will be retrieved! Queries that retrieve too many records at once can fail, so this allows the process to retrieve them in smaller chunks. The function will keep requesting records in chunks equal to this number until all matching records have been retrieved. If this value is too large (i.e., much greater than about \code{10000}), the server will likely respond with a 500 error. If \code{NULL} then all records will be retrieved in a single pass. Defaults to \code{NULL}.
#' @param delay Optional numeric. The number of milliseconds to wait between API queries. Querying too quickly can crash an API or get you locked out, so adjust this as needed. Defaults to \code{500}.
#' @param exact_match Logical. If \code{TRUE} then only records for which the provided keys are an exact match will be returned. If \code{FALSE} then records containing (but not necessarily matching exactly) the first provided key value will be returned e.g. searching with \code{exact_match = FALSE}, \code{keys = "42"}, and \code{key_type = "EcologicalSiteID"} would return all records in which the ecological site ID contained the string \code{"42"} such as \code{"R042XB012NM"} or \code{"R036XB042NM"}. If \code{FALSE} only the first provided key value will be considered. Using non-exact matching will dramatically increase server response times, so use with caution. Defaults to \code{TRUE}.
#' @param verbose Logical. If \code{TRUE} then the function will report additional diagnostic messages as it executes. Defaults to \code{FALSE}.
#' @returns A data frame of records from the requested \code{data_type} which contain the values from \code{keys} in the variable \code{key_type}.
#' @seealso
#' \code{\link[=fetch_ldc_spatial]{fetch_ldc_spatial()}} will query for data by spatial location.
#' \code{\link[=fetch_ldc_ecosite]{fetch_ldc_ecosite()}} will query for data by ecological site ID.
#' @examples
#' # To retrieve all sampling location metadata collected in the ecological sites R036XB006NM and R036XB007NM
#' headers <- fetch_ldc(keys = c("R036XB006NM", "R036XB007NM"), key_type = "EcologicalSiteID", data_type = "header")
#' # To retrieve all LPI data collected in ecological sites in the 036X Major Land Resource Area (MLRA)
#' relevant_headers <- fetch_ldc(keys = "036X", key_type = "EcologicalSiteID", data_type = "header", exact_match = FALSE)
#' lpi_data <- fetch_ldc(keys = relevant_headers$PrimaryKey, key_type = "PrimaryKey". data_type = "lpi", take = 10000)
#' @export
fetch_ldc <- function(keys = NULL,
                      key_type = NULL,
                      data_type,
                      username = NULL,
                      password = NULL,
                      token = NULL,
                      key_chunk_size = 100,
                      timeout = 300,
                      take = NULL,
                      delay = 500,
                      exact_match = TRUE,
                      verbose = FALSE) {
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  base_url <- "https://api.landscapedatacommons.org/api/v1/"
  valid_tables <- data.frame(data_type = c("gap",
                                           "header",
                                           "height",
                                           "lpi",
                                           "soilstability",
                                           "speciesinventory",
                                           "indicators",
                                           "species",
                                           "dustdeposition",
                                           "horizontalflux",
                                           "schema",
                                           "dataGap",
                                           "dataHeader",
                                           "dataHeight",
                                           "dataLPI",
                                           "dataSoilStability",
                                           "dataSpeciesInventory",
                                           "geoIndicators",
                                           "geoSpecies",
                                           "dataDustDeposition",
                                           "dataHorizontalFlux",
                                           "aerosummary",
                                           "dataAeroSummary",
                                           "rhem",
                                           "tblRHEM",
                                           "project",
                                           "tblProject"),
                             table_name = c("dataGap",
                                            "dataHeader",
                                            "dataHeight",
                                            "dataLPI",
                                            "dataSoilStability",
                                            "dataSpeciesInventory",
                                            "geoIndicators",
                                            "geoSpecies",
                                            "dataDustDeposition",
                                            "dataHorizontalFlux",
                                            "tbl-schema/latest",
                                            "dataGap",
                                            "dataHeader",
                                            "dataHeight",
                                            "dataLPI",
                                            "dataSoilStability",
                                            "dataSpeciesInventory",
                                            "geoIndicators",
                                            "geoSpecies",
                                            "dataDustDeposition",
                                            "dataHorizontalFlux",
                                            "dataAeroSummary",
                                            "dataAeroSummary",
                                            "tblRHEM",
                                            "tblRHEM",
                                            "tblProject",
                                            "tblProject"))
  if (!(data_type %in% valid_tables$data_type)) {
    stop(paste0("data_type must be one of the following character strings (some are aliases of each other): ",
                paste(valid_tables$data_type,
                      collapse = ", "),
                "."))
  }
  
  current_table <- valid_tables[["table_name"]][valid_tables$data_type == data_type]
  
  if (!(class(keys) %in% c("character", "NULL"))) {
    stop("keys must be a character string or vector of character strings or NULL.")
  }
  
  if (!(class(key_type) %in% c("character", "NULL"))) {
    stop("key_type must be a character string or NULL.")
  }
  
  if (!is.null(keys) & is.null(key_type)) {
    stop("Must provide key_type when providing keys.")
  }
  
  if (!is.null(take)) {
    if (!is.numeric(take) | length(take) > 1) {
      stop("take must either be NULL or a single numeric value.")
    }
  }
  
  if (delay < 0) {
    stop("delay must be a positive numeric value.")
  } else {
    # Convert the value from milliseconds to nanoseconds because we'll be using
    # microbenchmark::get_nanotime() which returns the current time in nanoseconds
    delay <- delay * 10^6
  }
  
  # Check the user credentials
  if (!is.null(token)) {
    if (class(token) == "list") {
      if (!("IdToken" %in% names(token))) {
        stop("A valid bearer ID token must be a single character string or a single character string in a list stored at an index named 'IdToken'.")
      } else if (class(token[["IdToken"]]) != "character") {
        stop("A valid bearer ID token must be a single character string or a single character string in a list stored at an index named 'IdToken'.")
      }
      if (!("expiration_time" %in% names(token))) {
        token[["expiration_time"]] <- Sys.time() + 3600
      }
    } else if (class(token) == "character") {
      if (length(token) != 1) {
        stop("A valid bearer ID token must be a single character string or a single character string in a list stored at an index named 'IdToken'.")
      }
      token <- list(IdToken = token,
                    # This is just a rough guess on the validity window of the
                    # token so we don't have to do more complicated handling
                    # later.
                    expiration_time = Sys.time() + 3600)
    }
  } else {
    if (!identical(is.null(username), is.null(password))) {
      if (is.null(username)) {
        warning("No token or username provided. Ignoring provided password and retrieving only data which do not require credentials.")
      }
      if (is.null(password)) {
        warning("No token or password provided. Ignoring provided username and retrieving only data which do not require credentials.")
      }
    } else if (!is.null(username) & !is.null(password)) {
      if (class(username) != "character" | length(username) > 1) {
        stop("Provided username must be a single character string.")
      }
      if (class(password) != "character" | length(password) > 1) {
        stop("Provided username must be a single character string.")
      }
      token <- get_ldc_token(username = username,
                             password = password)
    } else if (verbose) {
      message("No credentials provided. Retrieving only data which do not require credentials.")
    }
  }
  
  
  # If there are no keys, grab the whole table
  if (is.null(keys)) {
    if (verbose) {
      message("No keys provided; retrieving all records.")
    }
    if (!is.null(key_type)) {
      warning("No keys provided. Ignoring key_type and retrieving all records.")
    }
    queries <- paste0(base_url,
                      current_table)
  } else {
    # If there are keys, chunk them then build queries
    # This helps prevent queries so long that they fail
    if (verbose) {
      message("Grouping keys into chunks for queries.")
    }
    # We don't know whether the keys came in as a vector of single keys or if
    # one or more of the character strings contains keys separated by commas
    # so we're going to handle that an get a vector of single-key strings
    keys_vector <- unlist(lapply(X = keys,
                                 FUN = function(X) {
                                   trimws(unlist(stringr::str_split(string = X,
                                                                    pattern = ",")))
                                 }))
    # OKAY! So it turns out that it's not impossible for keys to contain
    # ampersands which will result in malformed API queries, so we'll replace
    # them with the unicode reference %26
    keys_vector_original <- keys_vector
    keys_vector <- gsub(x = keys_vector,
                 pattern = "[&]",
                 replacement = "%26")
    keys_vector <- gsub(x = keys_vector,
                        pattern = " ",
                        replacement = "%20")
    
    if (verbose & !identical(keys_vector_original, keys_vector)) {
      warning("Some keys provided contained illegal characters and have been sanitized. All available data should still be retrieved for all provided keys.")
    }
    
    if (!exact_match) {
      if (verbose) {
        message("Using non-exact matching for the key value.")
      }
      if (length(keys_vector) > 1) {
        warning("There are multiple provided key values. Non-exact matching will only consider the first.")
      }
      keys_vector <- keys_vector[1]
    }
    
    # Figure out how many chunks to break these into based on the max number of
    # keys in a chunk
    key_chunk_count <- ceiling(length(keys_vector) / key_chunk_size)
    
    # Make the key chunks
    # For each chunk, figure out the appropriate indices and paste together the
    # relevant key values into strings that we can use to build per-chunk queries
    keys_chunks <- sapply(X = 1:key_chunk_count,
                          keys_vector = keys_vector,
                          key_chunk_size = key_chunk_size,
                          key_count = length(keys_vector),
                          FUN = function(X, keys_vector, key_chunk_size, key_count) {
                            min_index <- max(c(1, (X - 1) * key_chunk_size + 1))
                            max_index <- min(c(key_count, X * key_chunk_siz
