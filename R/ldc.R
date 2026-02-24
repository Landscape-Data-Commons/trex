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
#' @param data_type Character string. The type of data to query. Note that the variable specified as \code{key_type} must appear in the table corresponding to \code{data_type}. Valid values are: \code{'gap'}, \code{'header'}, \code{'height'}, \code{'lpi'}, \code{'soilstability'}, \code{'speciesinventory'}, \code{'indicators'}, \code{'species'}, \code{'speciesinventory'}, \code{'plotchar'},\code{'aero'}, \code{'rhem'}, and \code{'schema'}.
#' @param username Optional character string. The username to supply to the Landscape Data Commons API. Some data in the Landscape Data Commons are accessible only to users with appropriate credentials. You do not need to supply credentials, but an API request made without them may return fewer or no data. This argument will be ignored if \code{password} is \code{NULL}. Defaults to \code{NULL}.
#' @param password Optional character string. The password to supply to the Landscape Data Commons API.  Some data in the Landscape Data Commons are accessible only to users with appropriate credentials. You do not need to supply credentials, but an API request made without them may return fewer or no data. This argument will be ignored if \code{username} is \code{NULL}. Defaults to \code{NULL}.
#' @param key_chunk_size Numeric. The number of keys to send in a single query. Very long queries fail, so the keys may be chunked into smaller queries with the results of all the queries being combined into a single output. Defaults to \code{100}.
#' @param timeout Numeric. The number of seconds to wait for a nonresponse from the API before considering the query to have failed. Defaults to \code{300}.
#' @param take Optional numeric. The number of records to retrieve at a time. This is NOT the total number of records that will be retrieved! Queries that retrieve too many records at once can fail, so this allows the process to retrieve them in smaller chunks. The function will keep requesting records in chunks equal to this number until all matching records have been retrieved. If this value is too large (i.e., much greater than about \code{10000}), the server will likely respond with a 500 error. If \code{NULL} then all records will be retrieved in a single pass. Defaults to \code{10000}.
#' @param delay Optional numeric. The number of milliseconds to wait between API queries. Querying too quickly can crash an API or get you locked out, so adjust this as needed. Defaults to \code{2000} (2 seconds).
#' @param exact_match Logical. If \code{TRUE} then only records for which the provided keys are an exact match will be returned. If \code{FALSE} then records containing (but not necessarily matching exactly) the first provided key value will be returned e.g. searching with \code{exact_match = FALSE}, \code{keys = "42"}, and \code{key_type = "EcologicalSiteID"} would return all records in which the ecological site ID contained the string \code{"42"} such as \code{"R042XB012NM"} or \code{"R036XB042NM"}. If \code{FALSE} only the first provided key value will be considered. Using non-exact matching will dramatically increase server response times, so use with caution. Defaults to \code{TRUE}.
#' @param coerce Logical. If \code{TRUE} then the returned values will be coerced into the intended class when they don't match, e.g., if a date variable is a character string instead of a date. Defaults to \code{TRUE}. 
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
                      query_parameters,
                      data_type,
                      username = NULL,
                      password = NULL,
                      token = NULL,
                      key_chunk_size = 100,
                      timeout = 300,
                      take = 10000,
                      delay = 2000,
                      exact_match = TRUE,
                      coerce = TRUE,
                      verb = "POST",
                      base_url = "https://api.landscapedatacommons.org/api/v1/",
                      verbose = FALSE) {
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  # base_url <- "https://api.landscapedatacommons.org/api/v1/"
  
  # This list stores the actual name of the table as understood by the API as
  # the index names and the aliases understood by trex as the vectors of values
  valid_tables <- list("dataGap" = c("gap", "dataGap"),
    "dataHeader" = c("header", "dataHeader"),
    "dataHeight" = c("height", "heights", "dataHeight"),
    "dataLPI" = c("lpi", "LPI", "dataLPI"),
    "dataSoilStability" = c("soilstability", "dataSoilStability"),
    "dataSpeciesInventory" = c("speciesinventory", "dataSpeciesInventory"),
    "geoIndicators" = c("indicators", "geoIndicators"),
    "geoSpecies" = c("species", "geoSpecies"),
    "dataAeroSummary" = c("aero", "AERO", "aerosummary", "dataAeroSummary"),
    "dataPlotCharacterization" = c("plotchar", "plotcharacterization", "dataPlotCharacterization"),
    "dataSoilHorizons" = c("soil", "soilhorizons", "dataSoilHorizons"),
    "tblRHEM" = c("rhem", "RHEM", "tblRHEM"),
    "tblProject" = c("project", "projects", "tblProject"))
  
  # This converts it to a data frame.
  valid_tables <- lapply(X = names(valid_tables),
                         valid_tables = valid_tables,
                         FUN = function(X, valid_tables){
                           data.frame(table_name = X,
                                      data_type = valid_tables[[X]])
                         }) |>
    dplyr::bind_rows()
  
  if (!(data_type %in% valid_tables$data_type)) {
    stop(paste0("data_type must be one of the following character strings (some are aliases of each other): ",
                paste(valid_tables$data_type,
                      collapse = ", "),
                "."))
  }
  
  current_table <- valid_tables[["table_name"]][valid_tables$data_type == data_type]
  
  if (!(class(keys) %in% c("character", "NULL"))) {
    stop("keys must be a character string, vector of character strings, or NULL.")
  }
  
  if (!(class(key_type) %in% c("character", "NULL"))) {
    stop("key_type must be a character string or NULL.")
  }
  
  if (!is.null(keys) & is.null(key_type)) {
    stop("Must provide key_type when providing keys.")
  }
  
  # This uses the metadata to confirm that the query won't return an error due
  # to referencing a variable that doesn't exist in the requested table.
  if (class(key_type) == "character") {
    available_variables <- fetch_ldc_metadata(data_type = data_type) |>
      dplyr::pull(.data = _,
                  var = "field")
    if (!(key_type %in% available_variables)) {
      stop(paste0("The variable ", key_type, " does not appear in ", current_table,
                  ". Possible valid key_type values for ", current_table, " are: ",
                  paste(available_variables,
                        collapse = ", ")))
    }
  }
  
  if (!is.null(take)) {
    if (!is.numeric(take) | length(take) > 1) {
      stop("take must either be NULL or a single numeric value.")
    }
    if (take > 10000) {
      warning(paste0("The current take value (", take, ") is large enough that there may be errors when retrieving data. Consider setting take to 10000 or less."))
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
  token <- check_token(token = token,
                       username = username,
                       password = password,
                       verbose = verbose)
  if (verbose & is.null(token)) {
    message("No credentials provided. Retrieving only data which do not require credentials.")
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
                            max_index <- min(c(key_count, X * key_chunk_size))
                            indices <- min_index:max_index
                            paste(keys_vector[indices],
                                  collapse = ",")
                          })
    
    if (verbose) {
      if (length(keys_chunks == 1)) {
        message("Building query.")
      } else {
        message("Building queries.")
      }
    }
    
    if (exact_match) {
      queries <- paste0(base_url,
                        current_table,
                        "?",
                        key_type,
                        "=",
                        keys_chunks)
    } else {
      # This adds "Like" to the end of the variable name to do a search for a
      # non-exact match. The object is still called "queries" even though it
      # had better be a single string instead of a vector.
      queries <- paste0(base_url,
                        current_table,
                        "?",
                        key_type,
                        "Like=",
                        keys_chunks)
    }
  }
  
  # Use the queries to snag data
  # This produces a list of results where each index in the list contains the
  # results of one query.
  # It uses a loop instead of a lapply() so that we can check if the token has
  # expired each time we use it.
  data_list <- list()
  
  querying_start_time <- Sys.time()
  
  for (current_query in queries) {
    token <- check_token(token = token,
                         username = username,
                         password = password,
                         verbose = verbose)
    # The token might expire and need refreshing!
    # if (!is.null(token)) {
    #   if (Sys.time() > token[["expiration_time"]]) {
    #     if (verbose) {
    #       message("Current API bearer authorization token has expired. Attempting to request a new one.")
    #     }
    #     if (!is.null(username) & !is.null(password)) {
    #       token <- get_ldc_token(username = username,
    #                              password = password)
    #     } else {
    #       warning("The API bearer authorization token has expired. Because username and password have not been provided, only data which do not require a token will be retrieved.")
    #       token <- NULL
    #     }
    #   }
    # }
    
    # We handle things differently if there's no take value
    if (is.null(take)) {
      if (verbose) {
        message("Attempting to query LDC with:")
        message(current_query)
      }
      
      content_df <- query_ldc(query = current_query,
                              token = token,
                              timeout = 300)
      
      # # Full query response using the token if we've got one.
      # if (is.null(token)) {
      #   response <- httr::GET(url = current_query,
      #                         httr::timeout(timeout),
      #                         httr::user_agent(user_agent))
      #   
      # } else {
      #   response <- httr::GET(url = current_query,
      #                         httr::timeout(timeout),
      #                         httr::user_agent(user_agent),
      #                         httr::add_headers(Authorization = paste("Bearer",
      #                                                                 token[["IdToken"]])))
      # }
      # 
      # if (verbose) {
      #   message("Response received from server. Attempting to parse it into a data frame.")
      # }
      # 
      # # What if there's an error????
      # if (httr::http_error(response)) {
      #   if (response$status_code == 500) {
      #     stop(paste0("Query failed with status ",
      #                 response$status_code,
      #                 " which may be due to a very large number of records returned or attempting to query using a variable that doesn't occur in the requested data table. Consider setting the take argument to 10000 or less and consult https://api.landscapedatacommons.org/api-docs to see which variables are in which tables."))
      #   } else {
      #     stop(paste0("Query failed with status ",
      #                 response$status_code))
      #   }
      # }
      # 
      # # Grab only the data portion
      # response_content <- response[["content"]]
      # # Convert from raw to character
      # content_character <- rawToChar(response_content)
      # # Convert from character to data frame
      # content_df <- jsonlite::fromJSON(content_character) |>
      #   as.data.frame(x = _)
      
    } else {
      # OKAY! So handling using take and cursor options for
      # anything non-header
      # The first query needs to not specify the cursor position
      # and then after that we'll keep trying with the last
      # rid value as the cursor until we get an empty
      # response
      if (verbose) {
        message(paste0("Retrieving records in chunks of ", take))
      }
      
      # Gotta make sure that we use a ? if there are no keys being passed or a
      # "&" if there are.
      query_contains_questionmark <- stringr::str_detect(string = current_query,
                                                         pattern = paste0(base_url,
                                                                          current_table,
                                                                          "\\?"))
      # if (verbose) {
      #   if (query_contains_questionmark) {
      #     message("This query already has a ? due to the presence of other parameters, using an &.")
      #   } else {
      #     message("This query does not already have a ? due to a lack of other parameters, using an ?.")
      #   }
      # }
      
      
      # So this uses either ? or & to add the take value to the query depending
      # on if there was a ? present in the query already.
      current_query <- paste0(current_query,
                              ifelse(test = query_contains_questionmark,
                                     yes = "&",
                                     no = "?"),
                              "take=", take)
      
      if (verbose) {
        message("Attempting to query LDC with:")
        message(current_query)
      }
      
      current_content_df <- query_ldc(query = current_query,
                                      token = token,
                                      timeout = timeout)
      
      
      content_df_list <- list(current_content_df)
      
      # Here's where we start iterating as long as we're still
      # getting data
      # So while the last returned response wasn't empty,
      # keep requesting the next response where the cursor
      # is set to the rid following the the highest rid in
      # the last chunk
      while (nrow(content_df_list[[length(content_df_list)]]) == take) {
        if (verbose) {
          message("The server may have additional qualifying records. Attempting to request them.")
        }
        # And to avoid flooding the API server with requests,
        # we'll put in a delay here.
        # This gets the current time then spins its wheels,
        # checking repeatedly to see if enough time has
        # elapsed, at which point it moves on
        start_time <- microbenchmark::get_nanotime()
        repeat {
          current_time <- microbenchmark::get_nanotime()
          elapsed_time <- current_time - start_time
          if (elapsed_time > delay) {
            break
          }
        }
        
        # CURSOR HANDLING!!!!!!!!!!!!
        # There's been some back-and-forth with the API on how to specify the
        # cursor position. Right now (2025-06-13) the way it works is that the
        # cursor references the RID in the table.
        # Example:
        # Submitted request:
        #    /api/v1/dataLPI?take=1&cursor=496815
        # Server-side SQL query:
        #    SELECT *
        #    FROM public_test.datalpi_filtered_view
        #    WHERE 1 = 1 AND "rid" > 496815 ORDER BY rid ASC LIMIT 1
        # So, the strategy here is just to use the highest value for RID in the
        # last returned records as the cursor for the next request.
        cursor_position <- dplyr::last(content_df_list) |>
          dplyr::pull(.data = _,
                      var = rid) |>
          max()
        
        # NO LONGER CORRECT, BUT HELD HERE JUST IN CASE
        # The cursor is based on the indices that the user can see, so the
        # previous solution using the RIDs won't work when the user's access is
        # restricted to a subset of the data.
        # cursor_position <- sapply(X = content_df_list,
        #                           FUN = nrow) |>
        #   sum()
        
        current_next_query <- paste0(current_query, "&cursor=", cursor_position)
        
        if (verbose) {
          message("Attempting to query LDC with:")
          message(current_next_query)
        }
        
        token <- check_token(token = token,
                             username = username,
                             password = password,
                             verbose = verbose)
        
        current_content_df <- query_ldc(query = current_next_query,
                                        token = token,
                                        timeout = timeout)
        
        
        # Bind that onto the end of the list
        # The data are wrapped in list() so that it gets added
        # as a data frame instead of as a vector for each variable
        content_df_list <- c(content_df_list, list(current_content_df))
      }
      content_df <- do.call(rbind,
                            content_df_list)
      
      # And another delay for between individual queries
      # that were generated by the key chunking instead of
      # by take
      start_time <- microbenchmark::get_nanotime()
      repeat {
        current_time <- microbenchmark::get_nanotime()
        elapsed_time <- current_time - start_time
        if (elapsed_time > delay) {
          break
        }
      }
    }
    # Append whatever it is that we got back to our collection of results from
    # the assorted queries.
    # This is important because some queries may actually take multiple requests
    # if there's a non-NULL take value, so each index in data_list will be the
    # results from the original query (based on key chunks) or that plus any
    # follow-up queries made due to take.
    data_list <- c(data_list, list(content_df))
  }
  
  # This is so that we can tell the user how long it took to grab all the data.
  querying_end_time <- Sys.time()
  
  total_time <- querying_end_time - querying_start_time
  time_units <- "seconds"
  if (total_time > 60) {
    total_time <- total_time / 60
    time_unit <- "minutes"
  }
  if (total_time > 60) {
    total_time <- total_time / 60
    time_unit <- "hours"
  }
  
  if (verbose) {
    message(paste0("The total time spent retrieving data from the server was: ",
                   round(total_time,
                         digits = 2), " ", time_units))
  }
  
  # Combine all the results of the queries
  data <- do.call(rbind,
                  data_list)
  
  # If there aren't data, let the user know
  if (length(data) < 1) {
    warning("No data retrieved. Confirm that your keys and key_type are correct and that you've provided valid credentials (if the data are not publicly accessible).")
    return(NULL)
  } else {
    # If there are data and the user gave keys, find which if any are missing
    if (!is.null(keys) & exact_match) {
      # Note that we're using keys_vector_original because even if we made
      # alterations to keys_vector, the actual retrieved keys should match the
      # original values despite substituting unicode references for illegal characters
      missing_keys <- keys_vector_original[!(keys_vector_original %in% data[[key_type]])]
      if (length(missing_keys) > 0) {
        warning(paste0(if (is.null(token) & (is.null(username) | is.null(password))) {
          "Some keys were not associated with publicly-available data. Using LDC credentials (either username and password or a token) may return data for the following keys: "
        } else {
          "The following keys were not associated with returned data using the provided LDC credentials: "
        },
        paste(missing_keys,
              collapse = ",")))
      }
    }
  }
  
  # Coerce as necessary and requested!
  if (coerce) {
    data <- coerce_ldc(data = data,
                       data_type = data_type,
                       verbose = verbose)
  }
  
  data
}


#' Fetching data from the Landscape Data Commons using spatial constraints
#' @description A function for retrieving data from the Landscape Data Commons which fall within a given set of polygons. This is accomplished by retrieving the header information for all points in the LDC, spatializing them, and finding the PrimaryKey values associated with points within the given polygons. Those PrimaryKey values are used to retrieve only the qualifying data from the LDC.
#' 
#' Every time this function is called, it retrieves ALL header information via the API, which can be slow. If you plan to do multiple spatial queries back-to-back, it'll be faster to retrieve the headers with \code{\link[=fetch_ldc]{fetch_ldc()}} once, convert them to an sf object with \code{sf::st_as_sf()}, then use \code{sf:st_intersection()} repeatedly on that sf object to find the PrimaryKey values for each set of polygons and query the API using the PrimaryKeys.
#' 
#' When you already know the associated PrimaryKeys, use \code{\link[=fetch_ldc]{fetch_ldc()}} instead. If you want to retrieve data associated with specific ecological site IDs, use \code{\link[=fetch_ldc_ecosite]{fetch_ldc_ecosite()}}.
#' @param polygons Polygon sf object. The polygon or polygons describing the area to retrieve data from. Only records from sampling locations falling within this area will be returned.
#' @param data_type Character string. The type of data to query. Note that the variable specified as \code{key_type} must appear in the table corresponding to \code{data_type}. Valid values are: \code{'gap'}, \code{'header'}, \code{'height'}, \code{'lpi'}, \code{'soilstability'}, \code{'speciesinventory'}, \code{'indicators'}, \code{'species'}, \code{'dustdeposition'}, \code{'horizontalflux'}, and \code{'schema'}.
#' @param username Optional character string. The username to supply to the Landscape Data Commons API. Some data in the Landscape Data Commons are accessible only to users with appropriate credentials. You do not need to supply credentials, but an API request made without them may return fewer or no data. This argument will be ignored if \code{password} is \code{NULL}. Defaults to \code{NULL}.
#' @param password Optional character string. The password to supply to the Landscape Data Commons API.  Some data in the Landscape Data Commons are accessible only to users with appropriate credentials. You do not need to supply credentials, but an API request made without them may return fewer or no data. This argument will be ignored if \code{username} is \code{NULL}. Defaults to \code{NULL}.
#' @param key_chunk_size Numeric. The number of PrimaryKeys to send in a single query. Very long queries fail, so the keys may be chunked into smaller queries with the results of all the queries being combined into a single output. Defaults to \code{100}.
#' @param timeout Numeric. The number of seconds to wait for a nonresponse from the API before considering the query to have failed. Defaults to \code{300}.
#' @param take Optional numeric. The number of records to retrieve at a time. This is NOT the total number of records that will be retrieved! Queries that retrieve too many records at once can fail, so this allows the process to retrieve them in smaller chunks. The function will keep requesting records in chunks equal to this number until all matching records have been retrieved. If this value is too large (i.e., much greater than about \code{10000}), the server will likely respond with a 500 error. If \code{NULL} then all records will be retrieved in a single pass. Defaults to \code{NULL}.
#' @param delay Optional numeric. The number of milliseconds to wait between API queries. Querying too quickly can crash an API or get you locked out, so adjust this as needed. Defaults to \code{500}.
#' @param return_spatial Logical. If \code{TRUE} then the returned data will be an sf object. Otherwise if this is \code{FALSE} it will be a simple data frame. Defaults to \code{TRUE}.
#' @param verbose Logical. If \code{TRUE} then the function will report additional diagnostic messages as it executes. Defaults to \code{FALSE}.
#' @returns A data frame of records from the requested \code{data_type} which came from locations within \code{polygons}.
#' @seealso
#' \code{\link[=fetch_ldc]{fetch_ldc()}} will query for data by any key values.
#' \code{\link[=fetch_ldc_ecosite]{fetch_ldc_ecosite()}} will query for data by ecological site ID.
#' @examples
#' To retrieve all LPI records for sampling locations found within a given set of polygons provided as an sf object
#' fetch_ldc_spatial(polygons = polygons_sf, data_type = "lpi")
#' @export
fetch_ldc_spatial <- function(polygons,
                              data_type,
                              token = NULL,
                              username = NULL,
                              password = NULL,
                              key_chunk_size = 100,
                              timeout = 300,
                              take = NULL,
                              delay =  500,
                              return_spatial = TRUE,
                              verbose = FALSE) {
  if (!("sf" %in% class(polygons))) {
    stop("polygons must be a polygon sf object")
  }
  
  # Just to get a unique ID in there for sure without having to ask the user
  polygons$unique_id <- 1:nrow(polygons)
  # Make sure that we reflect the inherent assumptions here.
  sf::st_agr(x = polygons) <- "constant"
  # And get the polygons into the correct CRS
  polygons <- sf::st_transform(x = polygons,
                               crs = "+proj=longlat +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +no_defs +type=crs")
  
  if (verbose) {
    message("Fetching the header information from the LDC.")
  }
  headers_df <- fetch_ldc(data_type = "header",
                          token = token,
                          username = username,
                          password = password,
                          verbose = verbose,
                          timeout = timeout)
  
  # We know that the header info includes coordinates in NAD83, so we can easily
  # convert the data frame into an sf object
  if (verbose) {
    message("Converting header information into an sf point object.")
  }
  headers_sf <- sf::st_as_sf(x = headers_df,
                             coords = c("Longitude_NAD83",
                                        "Latitude_NAD83"),
                             crs = "+proj=longlat +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +no_defs +type=crs")
  # Make sure that we reflect the inherent assumptions here.
  sf::st_agr(x = headers_sf) <- "constant"
  
  # We're just after the PrimaryKey values here
  if (verbose) {
    message("Finding points that fall within the polygons.")
  }
  header_polygons_intersection <- sf::st_intersection(x = headers_sf[, "PrimaryKey"],
                                                      y = polygons[, "unique_id"])
  
  # What if there're no qualifying data????
  if (nrow(header_polygons_intersection) < 1) {
    warning("No data were located within the given polygons.")
    return(NULL)
  }
  
  # If there were points found, snag the PrimaryKey values
  intersected_primarykeys <- unique(header_polygons_intersection$PrimaryKey)
  
  # Grab only the data associated with the PrimaryKey values we've got
  if (data_type == "header") {
    output <- headers_sf[headers_sf$PrimaryKey %in% intersected_primarykeys, ]
    if (!return_spatial) {
      output <- sf::st_drop_geometry(output)
    }
  } else {
    output <- fetch_ldc(keys = intersected_primarykeys,
                        key_type = "PrimaryKey",
                        data_type = data_type,
                        token = token,
                        username = username,
                        password = password,
                        key_chunk_size = key_chunk_size,
                        timeout = timeout,
                        exact_match = TRUE,
                        delay = delay,
                        verbose = verbose)
    if (return_spatial) {
      output <- dplyr::inner_join(x = dplyr::select(.data = headers_sf,
                                                    PrimaryKey),
                                  y = output)
    }
  }
  
  output
}

#' Fetching data from the Landscape Data Commons via API query using ecological site IDs
#' @description This is a wrapper for \code{\link[=fetch_ldc]{fetch_ldc()}} which streamlines retrieving data by ecological site IDs. Most tables in the LDC do not include ecological site information and so this function will identify the PrimaryKeys associated with the requested ecological site(s) and retrieve the requested data associated with those PrimaryKeys.
#' 
#' When you already know the associated PrimaryKeys, use \code{\link[=fetch_ldc]{fetch_ldc()}} instead. If you want to retrieve data associated with polygons, use \code{\link[=fetch_ldc_spatial]{fetch_ldc_spatial()}}.
#' @param keys Character vector. All the ecological site IDs (e.g. \code{"R036XB006NM"}) to search for. The returned data will consist only of records where the designated ecological site ID matched one of these values, but there may be ecological site IDS that return no records.
#' @param data_type Character string. The type of data to query. Valid values are: \code{'gap'}, \code{'header'}, \code{'height'}, \code{'lpi'}, \code{'soilstability'}, \code{'speciesinventory'}, \code{'indicators'}, \code{'species'}, \code{'dustdeposition'}, \code{'horizontalflux'}, and \code{'schema'}.
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
#' \code{\link[=fetch_ldc]{fetch_ldc()}} will query for data by any key values.
#' \code{\link[=fetch_ldc_spatial]{fetch_ldc_spatial()}} will query for data by spatial location.
#' @examples
#' # To retrieve all LPI records associated with the ecological sites R036XB006NM and R036XB007NM
#' fetch_ldc_ecosite(keys = c("R036XB006NM", "R036XB007NM"), data_type = "lpi")
#' @export
fetch_ldc_ecosite <- function(keys,
                              data_type,
                              token = NULL,
                              username = NULL,
                              password = NULL,
                              key_chunk_size = 100,
                              timeout = 300,
                              take = NULL,
                              delay = 500,
                              exact_match = TRUE,
                              verbose = FALSE) {
  # First order of business: grab the header info for sampling locations that
  # match the ecosite(s) requested
  if (verbose) {
    message("Retrieving header information")
  }
  current_headers <- fetch_ldc(keys = keys,
                               key_type = "EcologicalSiteID",
                               data_type = "header",
                               token = token,
                               username = username,
                               password = password,
                               key_chunk_size = key_chunk_size,
                               timeout = timeout,
                               take = NULL,
                               delay = 500,
                               exact_match = exact_match,
                               verbose = verbose)
  
  # Okay, so what if we get no data?
  # fetch_ldc() should already have warned the user, so we can just return NULL
  # Or if they wanted the headers, we just serve those out
  if (is.null(current_headers) | data_type == "header") {
    return(current_headers)
  }
  
  # Gimme those PrimaryKeys
  current_primarykeys <- unique(current_headers$PrimaryKey)
  
  if (verbose) {
    message("Retrieving requested data with relevant PrimaryKeys.")
  }
  # Grab the relevant data with the PrimaryKeys
  fetch_ldc(keys = current_primarykeys,
            key_type = "PrimaryKey",
            data_type = data_type,
            token = token,
            username = username,
            password = password,
            key_chunk_size = key_chunk_size,
            timeout = timeout,
            take = take,
            delay = delay,
            exact_match = TRUE,
            verbose = verbose)
}

#' Fetching data from the Landscape Data Commons via API query
#' @description A function for making API calls to the Landscape Data Commons based on the table, key variable, and key variable values. It will return a table of records of the requested data type from the LDC in which the variable \code{key_type} contains only values found in \code{keys}. See the \href{https://api.landscapedatacommons.org/api-docs}{API documentation} to see which variables (i.e. \code{key_type} values) are valid for each data type.
#' 
#' There are additional functions to simplify querying by spatial location (\code{\link[=fetch_ldc_spatial]{fetch_ldc_spatial()}}) and by ecological site ID (\code{\link[=fetch_ldc_ecosite]{fetch_ldc_ecosite()}}).
#' @param data_type Character string. The type of data to query. Note that the variable specified as \code{key_type} must appear in the table corresponding to \code{data_type}. Valid values are: \code{'gap'}, \code{'header'}, \code{'height'}, \code{'lpi'}, \code{'soilstability'}, \code{'speciesinventory'}, \code{'indicators'}, \code{'species'}, \code{'speciesinventory'}, \code{'plotchar'},\code{'aero'}, \code{'rhem'}, and \code{'schema'}.
#' @param timeout Numeric. The number of seconds to wait for a nonresponse from the API before considering the query to have failed. Defaults to \code{300}.
#' @param delay Optional numeric. The number of milliseconds to wait between API queries. Querying too quickly can cause issues, so adjust this as needed. Defaults to \code{500}.
#' @param verbose Logical. If \code{TRUE} then the function will report additional diagnostic messages as it executes. Defaults to \code{FALSE}.
#' @returns A data frame of metadata for the requested table. The data frame will contain all variables returned from the server plus an additional character variable called \code{"data_class_r"} which contains the R equivalent to the class of the variable called \code{"data_type"}.
#' @export
fetch_ldc_metadata <- function(data_type,
                               timeout = 300,
                               delay = 500,
                               verbose = FALSE) {
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  base_url <- "https://api.landscapedatacommons.org/api/v1/tblSchemaplan?table_name="
  
  # This list stores the actual name of the table as understood by the API as
  # the index names and the aliases understood by trex as the vectors of values
  valid_tables <- list(#"schema" = c("tbl-schema"),
    "dataGap" = c("gap", "dataGap"),
    "dataHeader" = c("header", "dataHeader"),
    "dataHeight" = c("height", "heights", "dataHeight"),
    "dataLPI" = c("lpi", "LPI", "dataLPI"),
    "dataSoilStability" = c("soilstability", "dataSoilStability"),
    "dataSpeciesInventory" = c("speciesinventory", "dataSpeciesInventory"),
    "geoIndicators" = c("indicators", "geoIndicators"),
    "geoSpecies" = c("species", "geoSpecies"),
    "dataAeroSummary" = c("aero", "AERO", "aerosummary", "dataAeroSummary"),
    "dataPlotCharacterization" = c("plotchar", "plotcharacterization", "dataPlotCharacterization"),
    "dataSoilHorizons" = c("soil", "soilhorizons", "dataSoilHorizons"),
    "tblRHEM" = c("rhem", "RHEM", "tblRHEM"),
    "tblProject" = c("project", "projects", "tblProject"))
  
  # This converts it to a data frame.
  # It's a distinct second step because I need X to be the actual table name and
  # not the vector of aliases so that I can add the proper name in a variable in
  # each data frame.
  valid_tables <- lapply(X = names(valid_tables),
                         valid_tables = valid_tables,
                         FUN = function(X, valid_tables){
                           data.frame(table_name = X,
                                      data_type = valid_tables[[X]])
                         }) |>
    dplyr::bind_rows()
  
  if (!(data_type %in% valid_tables$data_type)) {
    stop(paste0("data_type must be one of the following character strings (some are aliases of each other): ",
                paste(valid_tables$data_type,
                      collapse = ", "),
                "."))
  }
  
  # Just using that lookup table to snag the value that the server will
  # recognize.
  current_table <- valid_tables[["table_name"]][valid_tables$data_type == data_type]
  
  
  if (delay < 0) {
    stop("delay must be a positive numeric value.")
  } else {
    # Convert the value from milliseconds to nanoseconds because we'll be using
    # microbenchmark::get_nanotime() which returns the current time in nanoseconds
    delay <- delay * 10^6
  }
  
  # Build the query by slapping the table name on the end of the base URL.
  current_query <- paste0(base_url,
                          current_table)
  
  if (verbose) {
    message(paste0("Requesting metadata for ", current_table, " using the query: ",
                   current_query))
  }
  
  # Use the query to snag the table
  response <- httr::GET(url = current_query,
                        httr::timeout(timeout),
                        httr::user_agent(user_agent))
  
  # What if there's an error????
  if (httr::http_error(response)) {
    if (response$status_code == 500) {
      stop(paste0("Query failed with status ",
                  response$status_code,
                  " which may be due to a very large number of records returned or attempting to query using a variable that doesn't occur in the requested data table. Consider setting the take argument to 10000 or less and consult https://api.landscapedatacommons.org/api-docs to see which variables are in which tables."))
    } else {
      stop(paste0("Query failed with status ",
                  response$status_code))
    }
  }
  
  # Grab only the data portion
  response_content <- response[["content"]]
  # Convert from raw to character
  content_character <- rawToChar(response_content)
  # Convert from character to data frame.
  # This won't try to make a data frame if content_character is an empty string.
  if (nchar(content_character) > 0) {
    if (verbose) {
      message("Parsing server response.")
    }
    # This is a lookup between the strings that are found in the data_type
    # variable in the returned metadata table and what the R equivalents are.
    api_to_r_type_vector <- c("TEXT" = "character",
                              "BIT" = "logical",
                              "INTEGER" = "integer",
                              "NUMERIC" = "numeric",
                              "DATE" = "date",
                              "POSTGIS.GEOMETRY" = "character")
    
    # Convert from character to data frame
    content_df <- jsonlite::fromJSON(content_character) |>
      as.data.frame(x = _) |>
      # This bit adds an R value type variable that we can use elsewhere to
      # coerce values into the expected data type.
      dplyr::mutate(.data = _,
                    data_class_r = stringr::str_replace_all(string = data_type,
                                                            pattern = api_to_r_type_vector))
  } else {
    content_df <- NULL
  }
  
  # If there aren't data, let the user know
  if (length(content_df) < 1) {
    warning("No metadata retrieved. This is likely due to the LDC API not serving metadata for the requested data type.")
    return(NULL)
  } else {
    return(content_df)
  }
}


#' A function for coercing data in a data frame into an expected format.
#' @description Sometimes the data retrieved from the Landscape Data Commons is all character strings even though some variables should at least be numeric. This will coerce the variables into the correct format either using the metadata schema available through the Landscape Data Commons API or by simply attempting to coerce everything to numeric.
#' @param data Data frame. The data to be coerced. This is often the direct output from \code{fetch_ldc()}.
#' @param data_type Character string. If this is a character string recognized by \code{fetch_ldc()} and \code{fetch_ldc_metadata()} then the schema will be retrieved from the LDC and used to coerce values. If this is \code{NULL} then any variable that can be coerced into numeric without producing NA values will be coerced. Valid values are: \code{'gap'}, \code{'header'}, \code{'height'}, \code{'lpi'}, \code{'soilstability'}, \code{'speciesinventory'}, \code{'indicators'}, \code{'species'}, \code{'speciesinventory'}, \code{'plotchar'},\code{'aero'}, \code{'rhem'}, and \code{'schema'}.
#' @param verbose Logical. If \code{TRUE} then the function will report additional diagnostic messages as it executes. Defaults to \code{FALSE}.
#' @returns The original data frame, \code{data}, with variables coerced as possible and necessary.
#' @export
coerce_ldc <- function(data,
                       # lookup_table = NULL,
                       # field_var = NULL,
                       # field_type_var = NULL,
                       data_type,
                       verbose = FALSE) {
  if (!is.null(data_type)) {
    if (verbose) {
      message("Requesting schema from the LDC.")
    }
    
    var_lut <- fetch_ldc_metadata(data_type = data_type,
                                  verbose = verbose) |>
      dplyr::select(.data = _,
                    tidyselect::all_of(x = c("field",
                                             "data_class_r")))
    
    if (verbose) {
      message("Coercing variables as needed.")
    }
    
    data_coerced <- dplyr::mutate(.data = data,
                                  dplyr::across(.cols = tidyselect::any_of(x = dplyr::filter(.data = var_lut,
                                                                                             data_class_r != "date") |>
                                                                             dplyr::pull(.data = _,
                                                                                         var = field)),
                                                .fns = ~ methods::as(object = .x,
                                                                     Class = var_lut[["data_class_r"]][var_lut[["field"]] == dplyr::cur_column()])),
                                  # Doing some manual work on the dates because
                                  # the previous step claims there's no method for
                                  # coercion as written.
                                  # They're going to be character strings but we
                                  # only care about the first 10 characters which
                                  # ought to be the date as "YYYY-MM-DD"
                                  dplyr::across(.cols = tidyselect::any_of(x = dplyr::filter(.data = var_lut,
                                                                                             data_class_r == "date") |>
                                                                             dplyr::pull(.data = _,
                                                                                         var = field)),
                                                .fns = ~ substr(x = .x,
                                                                start = 1,
                                                                stop = 10) |>
                                                  as.Date(x = _)))
    
    
    if (verbose) {
      message("Coercion complete.")
    }
    
  } else {
    if (verbose) {
      message("No data_type specified, so only minimal coercion is possible. Attempting to coerce any variables to numeric possible without producing NA values.")
    }
    # We're working without a lookup table here
    # So, the easiest approach is to look at variables one-by-one
    # For each, we'll coerce to numeric then check to see if that introduced NAs
    # If it didn't, we'll call it a success
    data_coerced <- data
    for (data_var in names(data_coerced)) {
      if (verbose) {
        message(paste0("Evaluating ",
                       data_var,
                       " for coercion."))
      }
      current_vector <- data_coerced[[data_var]]
      current_class <- class(current_vector)
      incoming_na_indices <- which(is.na(current_vector))
      
      if (current_class != "numeric") {
        if (verbose) {
          message(paste0("Attempting to coerce ",
                         data_var,
                         " to numeric."))
        }
        
        current_vector_coerced <- as.numeric(current_vector)
        coerced_na_indices <- which(is.na(current_vector_coerced))
        
        # We're checking to see not only if there are NA values but that the
        # indices of the NA values are the same as in the uncoerced data because
        # the uncoerced data may have had some NA values already
        if (identical(incoming_na_indices, coerced_na_indices)) {
          data_coerced[[data_var]] <- current_vector_coerced
          if (verbose) {
            message("Successfully coerced ",
                    data_var,
                    ".")
          }
        } else {
          warning(paste0("Coercing ",
                         data_var,
                         " from ",
                         current_class,
                         " to date would produce NA values. The variable will not be coerced."))
        }
        
      } else {
        message(paste0(data_var,
                       " is already numeric. Skipping coercion."))
      }
    }
  }
  
  # Return our coerced data
  data_coerced
}

# This isn't exported right now and is for internal use.
query_ldc <- function(query,
                      token = NULL,
                      timeout = 300){
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  # Full query response using the token if we've got one.
  if (is.null(token)) {
    response <- httr::GET(url = query,
                          httr::timeout(timeout),
                          httr::user_agent(user_agent))
    
  } else {
    response <- httr::GET(url = query,
                          httr::timeout(timeout),
                          httr::user_agent(user_agent),
                          httr::add_headers(Authorization = paste("Bearer",
                                                                  token[["IdToken"]])))
  }
  
  # What if there's an error????
  if (httr::http_error(response)) {
    if (response$status_code == 500) {
      stop(paste0("Query failed with status ",
                  response$status_code,
                  " which may be due to a very large number of records returned or attempting to query using a variable that doesn't occur in the requested data table. Consider setting the take argument to 10000 or less and using trex::fetch_ldc_metadata() to see which variables are in which tables."))
    } else if (response$status_code == 502) {
      stop(paste0("Query failed with status ",
                  response$status_code,
                  " which is likely due to a server-side issue. Please contact the LDC admin if this problem persists."))
    } else {
      stop(paste0("Query failed with status ",
                  response$status_code,
                  ". Please contact the LDC admin if this problem persists after troubleshooting."))
    }
  }
  
  # Grab only the data portion
  response_content <- response[["content"]]
  # Convert from raw to character
  content_character <- rawToChar(response_content)
  # Convert from character to data frame
  content_df <- jsonlite::fromJSON(content_character) |>
    as.data.frame(x = _)
  
  content_df
}

check_token <- function(token,
                        username = NULL,
                        password = NULL,
                        verbose = FALSE) {
  # Check the user credentials
  if (!is.null(token)) {
    if (verbose) {
      message("Checking provided token for validity.")
    }
    if (class(token) == "list") {
      if (!("IdToken" %in% names(token))) {
        stop("A valid bearer ID token must be a single character string or a single character string in a list stored at an index named 'IdToken'.")
      } else if (class(token[["IdToken"]]) != "character") {
        stop("A valid bearer ID token must be a single character string or a single character string in a list stored at an index named 'IdToken'.")
      }
      if (!("expiration_time" %in% names(token))) {
        if (verbose) {
          message("The token doesn't have an associated expiration time and will be assumed to be valid for the next 30 minutes which may cause issues. Consider using get_ldc_token() to get a token with an accurate expiration time.")
        }
        # Best guess for an expiration time, which, at 30 minutes, is quite
        # generous.
        token[["expiration_time"]] <- Sys.time() + 1800
      }
      if (verbose) {
        message("Provided token appears to be valid.")
      }
    } else if (class(token) == "character") {
      if (length(token) != 1) {
        stop("A valid bearer ID token must be a single character string or a single character string in a list stored at an index named 'IdToken'.")
      }
      if (verbose) {
        message("The token doesn't have an associated expiration time and will be assumed to be valid for the next 30 minutes which may cause issues. Consider using get_ldc_token() to get a token with an accurate expiration time.")
      }
      token <- list(IdToken = token,
                    # This is just a rough guess on the validity window of the
                    # token so we don't have to do more complicated handling
                    # later.
                    expiration_time = Sys.time() + 1800)
    }
  } else {
    if (!identical(is.null(username), is.null(password))) {
      if (is.null(username)) {
        warning("No token or username provided. Returning NULL instead of getting a token.")
      }
      if (is.null(password)) {
        warning("No token or password provided. Returning NULL instead of getting a token.")
      }
    } else if (!is.null(username) & !is.null(password)) {
      if (class(username) != "character" | length(username) > 1) {
        stop("Provided username must be a single character string.")
      }
      if (class(password) != "character" | length(password) > 1) {
        stop("Provided username must be a single character string.")
      }
      if (verbose) {
        message("Getting a token using the provided username and password.")
      }
      token <- get_ldc_token(username = username,
                             password = password)
    } else if (verbose) {
      message("No credentials provided, returning NULL instead of getting a token.")
    }
  }
  
  # The token might expire and need refreshing!
  if (!is.null(token)) {
    if (Sys.time() > token[["expiration_time"]]) {
      if (verbose) {
        message("Current API bearer authorization token has expired. Attempting to request a new one.")
      }
      if (!is.null(username) & !is.null(password)) {
        token <- get_ldc_token(username = username,
                               password = password)
      } else {
        warning("The API bearer authorization token has expired. Because username and password have not been provided, only data which do not require a token will be retrieved.")
        token <- NULL
      }
    }
  }
  
  token
}

ldc_table_aliases <- function(alias = NULL){
  aliases <- list("dataGap" = c("gap", "dataGap"),
       "dataHeader" = c("header", "dataHeader"),
       "dataHeight" = c("height", "heights", "dataHeight"),
       "dataLPI" = c("lpi", "LPI", "dataLPI"),
       "dataSoilStability" = c("soilstability", "dataSoilStability"),
       "dataSpeciesInventory" = c("speciesinventory", "dataSpeciesInventory"),
       "geoIndicators" = c("indicators", "geoIndicators"),
       "geoSpecies" = c("species", "geoSpecies"),
       "dataAeroSummary" = c("aero", "AERO", "aerosummary", "dataAeroSummary"),
       "dataPlotCharacterization" = c("plotchar", "plotcharacterization", "dataPlotCharacterization"),
       "dataSoilHorizons" = c("soil", "soilhorizons", "dataSoilHorizons"),
       "tblRHEM" = c("rhem", "RHEM", "tblRHEM"),
       "tblProject" = c("project", "projects", "tblProject"))
  
  if (is.null(alias)) {
    aliases
  } else {
    output <- names(aliases)[sapply(X = aliases,
                          alias = alias,
                          FUN = function(X, alias){
                            alias %in% X
                          })]
    if (length(output) < 1) {
      stop(paste0(alias, " is not a recognized alias for any table in the LDC. Please use trex::ldc_table_aliases() to see which aliases are recognized."))
    }
    output
  }
}
