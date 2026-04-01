#' Fetching data from the Landscape Data Commons via API query
#' @description A function for making API calls to the Landscape Data Commons based on the table, key variable, and key variable values. It will return a table of records of the requested data type from the LDC in which the variable \code{key_type} contains only values found in \code{keys}. See the \href{https://api.landscapedatacommons.org/api-docs}{API documentation} to see which variables (i.e. \code{key_type} values) are valid for each data type.
#' 
#' There are additional functions to simplify querying by spatial location (\code{\link[=fetch_ldc_spatial]{fetch_ldc_spatial()}}) and by ecological site ID (\code{\link[=fetch_ldc_ecosite]{fetch_ldc_ecosite()}}).
# #' @param keys Optional character vector. A character vector of all the values to search for in \code{key_type}. The returned data will consist only of records where \code{key_type} contained one of the key values, but there may be keys that return no records. If \code{NULL} then the entire table will be returned. Defaults to \code{NULL}.
# #' @param key_type Optional character string. The name of the variable in the data to search for the values in \code{keys}. This must be the name of a variable that exists in the requested data type's table, e.g. \code{"PrimaryKey"} exists in all tables, but \code{"EcologicalSiteID"} is found only in some. If the function returns a status code of 500 as an error, this variable may not be found in the requested data type. If \code{NULL} then the entire table will be returned. Defaults to \code{NULL}.
#' @param data_type Character string. The type of data to query. Note that any variables specified in \code{query_parameters} must appear in the table corresponding to \code{data_type}. To see all valid values, use \code{ldc_table_names()}.
#' @param query_parameters List. A list of query parameters which can be recognized by (\code{link[=format_query_parameters]{format_query_parameters()}}) and converted into a query for the API, e.g., \code{list("ProjectKey" = list("equals" = "BLM_AIM"))}. Defaults to an empty list which is equivalent to requesting all available records.
#' @param username Optional character string. The username associated with the LDC API account and which has been used to store one or more API keys using (\code{link[=store_api_key]{store_api_key()}}). This is almost certainly an email address. Some data in the Landscape Data Commons are accessible only to users with appropriate credentials. You do not need to supply credentials, but an API request made without them may return fewer or no data. Defaults to \code{NULL}.
#' @param api_key_name Optional character string. The name used to store the API key to submit with the query and which is associated with \code{username}. If this is \code{NULL} only data which do not require special permissions will be returned. Defaults to \code{NULL}.
#' @param token Logical. If \code{TRUE} then a token will be retrieved from the appropriate keyring for the supplied username. If the token is expired or does not exist, a new one will be requested. This feature will be deprecated when the LDC API stops supporting tokens in favor of API keys. Defaults to \code{FALSE}.
#' @param keyring_name Optional character string. The name of the keyring to attempt to retrieve passwords, keys, or tokens from. If \code{NULL} then the function will make a guess if needed. Defaults to \code{NULL}.
# #' @param password Optional character string. The password to supply to the Landscape Data Commons API.  Some data in the Landscape Data Commons are accessible only to users with appropriate credentials. You do not need to supply credentials, but an API request made without them may return fewer or no data. This argument will be ignored if \code{username} is \code{NULL}. Defaults to \code{NULL}.
# #' @param key_chunk_size Numeric. The number of keys to send in a single query. Very long queries fail, so the keys may be chunked into smaller queries with the results of all the queries being combined into a single output. Defaults to \code{100}.
#' @param timeout Numeric. The number of seconds to wait for a nonresponse from the API before considering the query to have failed. Defaults to \code{300}.
#' @param take Optional numeric. The number of records to retrieve at a time. This is NOT the total number of records that will be retrieved! Queries that retrieve too many records at once can fail, so this allows the process to retrieve them in smaller chunks. The function will keep requesting records in chunks equal to this number until all matching records have been retrieved. If this value is too large (i.e., much greater than about \code{10000}), the server will likely respond with a 500 error. If \code{NULL} then all records will be retrieved in a single pass. Defaults to \code{10000}.
#' @param delay Optional numeric. The number of milliseconds to wait between API queries. Querying too quickly can crash an API or get you locked out, so adjust this as needed. Defaults to \code{2000} (2 seconds).
# #' @param exact_match Logical. If \code{TRUE} then only records for which the provided keys are an exact match will be returned. If \code{FALSE} then records containing (but not necessarily matching exactly) the first provided key value will be returned e.g. searching with \code{exact_match = FALSE}, \code{keys = "42"}, and \code{key_type = "EcologicalSiteID"} would return all records in which the ecological site ID contained the string \code{"42"} such as \code{"R042XB012NM"} or \code{"R036XB042NM"}. If \code{FALSE} only the first provided key value will be considered. Using non-exact matching will dramatically increase server response times, so use with caution. Defaults to \code{TRUE}.
#' @param coerce Logical. If \code{TRUE} then the returned values will be coerced into the intended class when they don't match, e.g., if a date variable is a character string instead of a date. Defaults to \code{TRUE}. 
#' @param base_url Character string. The URL for the API endpoint to use. Defaults to \code{"https://api.landscapedatacommons.org/api/v1/"}.
#' @param multi_table_queries Logical. If \code{TRUE} then variables in \code{query_parameters} which do not occur in the table specified by \code{data_type} will be applied to other tables in the database as possible. The results of those ancillary queries are used to limit the PrimaryKey values in the data returned from the requested data type. If this is \code{FALSE} or any variables appear in no tables, an error will trigger. Defaults to \code{FALSE}.
#' @param ignored_tables Optional character vector. If \code{multi_table_queries} is \code{TRUE} these tables will not be queried even if they contain the relevant variables. This is useful for preventing querying multiple tables which share a variable, e.g. "LineLengthAmount" which occurs in the LPI, gap, and height data. Aliases recognized by \code{ldc_table_names()} are accepted. Defaults to \code{NULL}.
#' @param verbose Logical. If \code{TRUE} then the function will report additional diagnostic messages as it executes. Defaults to \code{FALSE}.
#' @returns A data frame of records from the requested \code{data_type} which contain the values from \code{keys} in the variable \code{key_type}.
#' @seealso
#' \code{\link[=fetch_ldc_spatial]{fetch_ldc_spatial()}} will query for data by spatial location.
#' \code{\link[=fetch_ldc_ecosite]{fetch_ldc_ecosite()}} will query for data by ecological site ID.
#' @examples
#' # To retrieve all sampling location metadata collected in the ecological
#' # sites R036XB006NM and R036XB007NM.
#' headers <- fetch_ldc(query_parameters = list("EcologicalSiteID" = list("=" = c("R036XB006NM",
#'                                                                                "R036XB007NM"))),
#'                                         data_type = "header")
#'                                         
#' # To retrieve all Line-Point Intercept collected between 40 and 45 degrees
#' # latitude (inclusive) and east of -115 degrees longitude (exclusive).
#' lpi_data <- fetch_ldc(query_parameters = list("Latitude_NAD83" = list(">=" = 40,
#'                                                                       "<=" = 45),
#'                                               "Longitude_NAD83" = list(">" = -115)),
#'                       data_type = "lpi")
#'                                         
#' @export
fetch_ldc <- function(data_type,
                      query_parameters = list(),
                      username = NULL,
                      api_key_name = NULL,
                      token = FALSE,
                      keyring_name = NULL,
                      timeout = 300,
                      take = 10000,
                      delay = 2000,
                      coerce = TRUE,
                      base_url = "https://api.landscapedatacommons.org/api/v1/",
                      multi_table_queries = FALSE,
                      ignored_tables = NULL,
                      verbose = FALSE,
                      keys = deprecated(),
                      key_type = deprecated(),
                      password = deprecated(),
                      key_chunk_size = deprecated(),
                      exact_match = deprecated()) {
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  
  if (lifecycle::is_present(keys) | lifecycle::is_present(key_type)) {
    lifecycle::deprecate_warn(when = "2.0.0",
                              what = I("Support for the `key_type` and `keys` arguments of `fetch_ldc()`"),
                              details = "These arguments have been deprecated in favor of query_parameters which supports multiple variables instead of just one.")
  }
  if (lifecycle::is_present(key_chunk_size)) {
    lifecycle::deprecate_warn(when = "2.0.0",
                              what = "fetch_ldc(key_chunk_size)",
                              details = "API queries now use POST which has rendered key_chunk_size irrelevant.")
  }
  if (lifecycle::is_present(password)) {
    lifecycle::deprecate_warn(when = "2.0.0",
                              what = "fetch_ldc(password)",
                              details = "The use of a plaintext password argument is no longer supported due to changes in LDC API credential handling. Please use the argument api_key_name or token in conjunction with setup_keyring() and store_api_key() instead.")
  }
  # if (lifecycle::is_present(token)) {
  #   lifecycle::deprecate_warn(when = "2.0.0",
  #                             what = "fetch_ldc(token)",
  #                             details = "Changes in LDC API credential handling mean that tokens are no longer supported. Please use the api_key_name argument in conjunction with setup_keyring() and store_api_key() instead.")
  # }
  if (lifecycle::is_present(exact_match)) {
    lifecycle::deprecate_warn(when = "2.0.0",
                              what = "fetch_ldc(exact_match)",
                              details = "Partial string matching is not currently supported by the LDC API so the behavior is now always equivalent to exact_match = TRUE.")
  }
  if (lifecycle::is_present(exact_match)) {
    lifecycle::deprecate_warn(when = "2.0.0",
                              what = "fetch_ldc(exact_match)",
                              details = "Partial string matching is not currently supported by the LDC API so the behavior is now always equivalent to exact_match = TRUE.")
  }
  
  if (is.null(username) & !is.null(api_key_name)) {
    warning("Although an api_key_name has been specified, no username has been provided and so no stored API key will be retrieved.")
    api_key_name <- NULL
  }
  
  if (token) {
    if (!is.null(api_key_name)) {
      if (verbose) {
        message("Because api_key_name was provided, a token will not be used.")
      }
      token <- FALSE
    } else if (is.null(username)) {
      #if (verbose) {
      warning("Because no username was provided, a token will not be used despite the argument token being set to TRUE.")
      # }
      token <- FALSE
    } else {
      if (verbose) {
        message("A stored token will be used if it exists.")
      }
    }
  }
  
  # Get the API-recognized name for the submitted alias
  data_type <- ldc_table_names(alias = data_type)
  
  # if (!(class(keys) %in% c("character", "NULL"))) {
  #   stop("keys must be a character string, vector of character strings, or NULL.")
  # }
  # 
  # if (!(class(key_type) %in% c("character", "NULL"))) {
  #   stop("key_type must be a character string or NULL.")
  # }
  # 
  # if (!is.null(keys) & is.null(key_type)) {
  #   stop("Must provide key_type when providing keys.")
  # }
  # 
  # # Make sure we've got a query_parameters list even if they're using the stupid
  # # old arguments from the dark ages
  # if (!any(sapply(X = list(keys, key_type), FUN = is.null))) {
  #   if (verbose) {
  #     message("Adding the provided keys to the query_parameters list. Consider using the query_parameter argument instead.")
  #   }
  #   query_parameters[[key_type]] <- list("=" = keys)
  # }
  
  # This uses the metadata to confirm that the query won't return an error due
  # to referencing a variable that doesn't exist in the requested table.
  
  
  # available_variables <- fetch_ldc_metadata(data_type = data_type) |>
  #   dplyr::pull(.data = _,
  #               var = "field")
  
  #### QUERY HANDLING ----------------------------------------------------------
  # This will (potentially) produce multiple parameter lists.
  # The "main" query will be the one with parameters only for the requested
  # data_type and the others will be used to get a single set of PrimaryKeys
  # from however many other tables are needed that will get appended to the main
  # query to represent those.
  # ldc_schema is data set included in this package.
  required_tables_lut <- dplyr::filter(.data = ldc_schema,
                                       field %in% names(query_parameters)) |>
    dplyr::select(.data = _,
                  tidyselect::all_of(c("table_name",
                                       "field"))) |>
    dplyr::arrange(.data = _,
                   field)
  
  # So the user can exclude tables from being queried.
  if (!is.null(ignored_tables)) {
    required_tables_lut <- dplyr::filter(.data = required_tables_lut,
                                         !(table_name %in% sapply(X = ignored_tables,
                                                                  FUN = ldc_table_names)))
  }
  
  main_query_parameters <- dplyr::filter(.data = required_tables_lut,
                                         table_name == data_type) |>
    dplyr::pull(.data = _,
                var = "field") |>
    query_parameters[.x = _]
  

  # This makes sure that we're preferentially querying the actual requested
  # table in situations where variables appear in multiple tables, e.g.
  # LineLengthAmount
  # if (limit_queries) {
  #   if (verbose) {
  #     message("Because limit_queries is TRUE, a query parameter involving a variable that occurs in multiple tables will be applied only to the requested data type or (in the case that it does not occur in that table) to only the first table listed in trex::ldc_schema that contains that variable.")
  #   }
  required_ancillary_tables_lut <- dplyr::filter(.data = required_tables_lut,
                                                 !(field %in% names(main_query_parameters))) |>
    dplyr::summarize(.data = _,
                     .by = field,
                     table_name = dplyr::first(table_name))
  # } else {
  #   
  #   warning("Because limit_queries is FALSE, a query parameter involving a variable that occurs in multiple tables will be applied EVERY table it occurs in according to trex::ldc_schema. This is risky and very likely to return no data at all because very few PrimaryKeys occur in every table.")
  #   
  #   required_ancillary_tables_lut <- dplyr::filter(.data = required_tables_lut,
  #                                                  table_name != data_type)
  # }
  
  
  ##### Ancillary queries ------------------------------------------------------
  ancillary_query_parameters <- setNames(object = setdiff(x = unique(required_ancillary_tables_lut$table_name),
                                                          y = data_type),
                                         nm = setdiff(x = unique(required_ancillary_tables_lut$table_name),
                                                      y = data_type)) |>
    lapply(X = _,
           required_ancillary_tables_lut = required_ancillary_tables_lut,
           query_parameters = query_parameters,
           FUN = function(X, required_ancillary_tables_lut, query_parameters){
             dplyr::filter(.data = required_ancillary_tables_lut,
                           table_name == X) |>
               dplyr::pull(.data = _,
                           field) |>
               query_parameters[.x = _]
           })
  
  if (!multi_table_queries) {
    if (!all(names(query_parameters) %in% names(main_query_parameters))) {
      stop(paste0("Some variables in query_parameters (", paste(setdiff(x = names(query_parameters),
                                                                        y = names(main_query_parameters)),
                                                                collapse = ", "), ") are not found in the requested data table (", data_type, "). Consider setting multi_table_queries to TRUE to try to use those parameters with other tables."))
    }
  } else {
    if (!all(names(query_parameters) %in% c(names(main_query_parameters), sapply(X = ancillary_query_parameters,
                                                                                 FUN = names)))) {
      stop(paste0("The following variables in query_parameters are not found in any LDC database table: ",
                  paste(setdiff(x = names(query_parameters),
                                y = trex::ldc_schema[["field"]]),
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
  } else {
    if (verbose) {
      message("No take value was specified. Defaulting to 10000.")
    }
    take <- 10000
  }
  
  # Add take to the parameters
  if (!is.null(take)) {
    main_query_parameters[["take"]] <- list("=" = take)
    
    if (length(ancillary_query_parameters) > 0) {
      if (!multi_table_queries) {
        stop(paste0("Some query parameters (", paste0(sapply(X = ancillary_query_parameters,
                                                             FUN = names) |>
                                                        unique() |>
                                                        setdiff(x = _,
                                                                y = "take"),
                                                      collapse = ", "), ") would require querying additional tables (", paste(names(ancillary_query_parameters),
                                                                                                                              collapse = ", "), "). To use those ancillary queries, set multi_table_queries to TRUE."))
      }
      ancillary_query_parameters <- lapply(X = ancillary_query_parameters,
                                           take = take,
                                           FUN = function(X, take){
                                             X[["take"]] <- list("=" = take)
                                             X
                                           })
    }
  }
  
  if (delay < 0) {
    stop("delay must be a positive numeric value.")
  } else {
    # Convert the value from milliseconds to nanoseconds because we'll be using
    # microbenchmark::get_nanotime() which returns the current time in nanoseconds
    delay <- delay * 10^6
  }
  
  if (verbose & is.null(api_key_name) & !token) {
    message("Retrieving only data which do not require credentials.")
  }
  
  # Just to keep track of the time elapsed.
  querying_start_time <- Sys.time()
  
  # First up, if there are ancillary queries to make to other tables, we'll do
  # that because we'll want to add the PrimaryKeys to the main query.
  if (length(ancillary_query_parameters) > 0) {
    if (verbose) {
      message(paste0("The query parameters span multiple data tables in the LDC database. Getting PrimaryKey values in common across the following tables associated with relevant parameters: ",
                     paste(names(ancillary_query_parameters),
                           collapse = ", ")))
    }
    # For each ancillary data table, we'll run the queries with the relevant
    # parameters and keep only the unique PrimaryKey values from those.
    primarykeys <- c()
    
    for (current_ancillary_table in names(ancillary_query_parameters)) {
      if (verbose) {
        message(paste0("Sending ancillary query regarding ", current_ancillary_table, "."))
      }
      data_list <- list()
      keep_querying <- TRUE
      current_ancillary_query_parameters <- ancillary_query_parameters[[current_ancillary_table]]
      while (keep_querying) {
        if (verbose) {
          message("Submitting ancillary query to the API.")
        }
        if (token) {
          current_token_status <- check_token(username = username,
                                              api_name = "ldc",
                                              keyring_name = keyring_name,
                                              verbose = verbose)
          
          if (current_token_status != "valid") {
            if (verbose) {
              message("Unable to find a valid stored LDC API token. Requesting and storing a new token.")
            }
            store_api_token(username = username,
                            api_name = "ldc",
                            keyring_name = keyring_name,
                            overwrite = TRUE,
                            verbose = verbose)
          }
          current_data <- query_ldc(data_type = current_ancillary_table,
                                    body = current_ancillary_query_parameters,
                                    token = get_stored_token(username = username,
                                                             api_name = "ldc",
                                                             keyring_name = keyring_name,
                                                             verbose = verbose),
                                    base_url = base_url,
                                    timeout = timeout,
                                    verbose = verbose)
        } else {
          current_data <- query_ldc(data_type = current_ancillary_table,
                                    body = current_ancillary_query_parameters,
                                    api_key = get_stored_key(username = username,
                                                             api_key_name = api_key_name,
                                                             keyring_name = keyring_name),
                                    base_url = base_url,
                                    timeout = timeout,
                                    verbose = verbose)
        }
        
        
        # Bind that onto the end of the list
        # The data are wrapped in list() so that it gets added
        # as a data frame instead of as a vector for each variable
        data_list <- c(data_list,
                       list(current_data))
        
        if (nrow(current_data) < take) {
          if (verbose) {
            message("All qualifying data available with the provided credentials have been returned.")
          }
          keep_querying <- FALSE
        } else {
          if (verbose) {
            message(paste0("The previous query returned exactly the maximum number of records with the current value for take (", take, "). Checking to see if there are additional qualifying records."))
          }
          current_ancillary_query_parameters[["cursor"]] <- list("=" = max(current_data$rid))
          # Should be unnecessary, but just to be safe!
          keep_querying <- TRUE
          
          # And to avoid flooding the API server with requests, we'll put in a delay
          # here.
          # This gets the current time then spins its wheels, checking repeatedly to
          # see if enough time has elapsed, at which point it moves on.
          start_time <- microbenchmark::get_nanotime()
          repeat {
            current_time <- microbenchmark::get_nanotime()
            elapsed_time <- current_time - start_time
            if (elapsed_time > delay) {
              break
            }
          }
        }
      }
      primarykeys <- dplyr::bind_rows(data_list) |>
        dplyr::pull(.data = _,
                    PrimaryKey) |>
        unique() |>
        c(.x = _,
          primarykeys) |>
        unique()
      
      if (length(primarykeys) > 0) {
        if (verbose) {
          message(paste0("Qualifying PrimaryKey values added to subsequent queries."))
        }
        
        ancillary_query_parameters <- lapply(X = ancillary_query_parameters,
                                             primarykeys = primarykeys,
                                             FUN = function(X, primarykeys){
                                               X[["PrimaryKey"]][["="]] <- c(X[["PrimaryKey"]][["="]],
                                                                             primarykeys) |>
                                                 unique()
                                               X
                                             })
        
        
      } else {
        warning("No PrimaryKeys met all the requirements across the other table(s) associated with their query parameters, so no records of the requested data type would be retrieved. Returning NULL.")
        return(NULL)
      }
    }
  }
  
  if (length(primarykeys) > 0) {
    if (verbose) {
      message(paste0("Qualifying PrimaryKey values added to the query for the table ", data_type, "."))
    }
    main_query_parameters[["PrimaryKey"]][["="]] <- c(main_query_parameters[["PrimaryKey"]][["="]],
                                                      primarykeys) |>
      unique()
  } else {
    warning("No PrimaryKeys met all the requirements across the other table(s) associated with their query parameters, so no records of the requested data type would be retrieved. Returning NULL.")
    return(NULL)
  }
  
  # Use the queries to snag data
  # This produces a list of results where each index in the list contains the
  # results of one query.
  # It uses a loop instead of a lapply() so that we can check if the token has
  # expired each time we use it.
  data_list <- list()
  
  
  # querying_start_time <- Sys.time()
  
  keep_querying <- TRUE
  data_list <- list()
  while (keep_querying) {
    if (verbose) {
      message("Submitting query to the API.")
    }
    if (token) {
      current_token_status <- check_token(username,
                                          api_name = "ldc",
                                          keyring_name = keyring_name,
                                          verbose = verbose)
      
      if (current_token_status != "valid") {
        if (verbose) {
          message("Unable to find a valid stored LDC API token. Requesting and storing a new token.")
        }
        store_api_token(username = username,
                        api_name = "ldc",
                        keyring_name = keyring_name,
                        overwrite = TRUE,
                        verbose = verbose)
      }
      current_data <- query_ldc(data_type = data_type,
                                body = main_query_parameters,
                                token = get_stored_token(username = username,
                                                         api_name = "ldc",
                                                         keyring_name = keyring_name,
                                                         verbose = verbose),
                                base_url = base_url,
                                timeout = timeout,
                                verbose = verbose)
    } else {
      current_data <- query_ldc(data_type = data_type,
                                body = main_query_parameters,
                                api_key = get_stored_key(username = username,
                                                         api_key_name = api_key_name,
                                                         keyring_name = keyring_name),
                                base_url = base_url,
                                timeout = timeout,
                                verbose = verbose)
    }
    
    
    # Bind that onto the end of the list
    # The data are wrapped in list() so that it gets added
    # as a data frame instead of as a vector for each variable
    data_list <- c(data_list,
                   list(current_data))
    
    if (nrow(current_data) < take) {
      if (verbose) {
        message("All qualifying data available with the provided credentials have been returned.")
      }
      keep_querying <- FALSE
    } else {
      if (verbose) {
        message(paste0("The previous query returned exactly the maximum number of records with the current value for take (", take, "). Checking to see if there are additional qualifying records."))
      }
      main_query_parameters[["cursor"]] <- list("=" = max(current_data$rid))
      # Should be unnecessary, but just to be safe!
      keep_querying <- TRUE
      
      # And to avoid flooding the API server with requests, we'll put in a delay
      # here.
      # This gets the current time then spins its wheels, checking repeatedly to
      # see if enough time has elapsed, at which point it moves on.
      start_time <- microbenchmark::get_nanotime()
      repeat {
        current_time <- microbenchmark::get_nanotime()
        elapsed_time <- current_time - start_time
        if (elapsed_time > delay) {
          break
        }
      }
    }
  }
  
  # This is so that we can tell the user how long it took to grab all the data.
  querying_end_time <- Sys.time()
  
  total_time <- querying_end_time - querying_start_time
  time_unit <- "seconds"
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
                         digits = 2), " ", time_unit))
  }
  
  # Combine all the results of the queries
  data <- dplyr::bind_rows(data_list)
  
  # If there aren't data, let the user know
  if (length(data) < 1) {
    warning("No data retrieved. Confirm that your query parameters are correct and that you've used a valid API key or token (if the data are not publicly accessible).")
    return(NULL)
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
# #' @param password Optional character string. The password to supply to the Landscape Data Commons API.  Some data in the Landscape Data Commons are accessible only to users with appropriate credentials. You do not need to supply credentials, but an API request made without them may return fewer or no data. This argument will be ignored if \code{username} is \code{NULL}. Defaults to \code{NULL}.
# #' @param key_chunk_size Numeric. The number of PrimaryKeys to send in a single query. Very long queries fail, so the keys may be chunked into smaller queries with the results of all the queries being combined into a single output. Defaults to \code{100}.
#' @param timeout Numeric. The number of seconds to wait for a nonresponse from the API before considering the query to have failed. Defaults to \code{300}.
#' @param take Optional numeric. The number of records to retrieve at a time. This is NOT the total number of records that will be retrieved! Queries that retrieve too many records at once can fail, so this allows the process to retrieve them in smaller chunks. The function will keep requesting records in chunks equal to this number until all matching records have been retrieved. If this value is too large (i.e., much greater than about \code{10000}), the server will likely respond with a 500 error. If \code{NULL} then all records will be retrieved in a single pass. Defaults to \code{NULL}.
#' @param delay Optional numeric. The number of milliseconds to wait between API queries. Querying too quickly can crash an API or get you locked out, so adjust this as needed. Defaults to \code{500}.
#' @param return_spatial Logical. If \code{TRUE} then the returned data will be an sf object. Otherwise if this is \code{FALSE} it will be a simple data frame. Defaults to \code{TRUE}.
#' @param base_url Character string. The URL for the API endpoint to use. Defaults to \code{"https://api.landscapedatacommons.org/api/v1/"}.
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
                              username = NULL,
                              api_key_name = NULL,
                              timeout = 300,
                              take = 10000,
                              delay =  500,
                              return_spatial = TRUE,
                              base_url = "https://api.landscapedatacommons.org/api/v1/",
                              verbose = FALSE,
                              password = deprecated(),
                              key_chunk_size = deprecated()) {
  if (!("sf" %in% class(polygons))) {
    stop("polygons must be a polygon sf object")
  }
  
  # Just to get a unique ID in there for sure without having to ask the user
  polygons$unique_id <- 1:nrow(polygons)
  # Make sure that we reflect the inherent assumptions here.
  sf::st_agr(x = polygons) <- "constant"
  # And get the polygons into the correct CRS
  polygons <- sf::st_transform(x = polygons,
                               crs = "+proj=longlat +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +no_defs +type=crs") |>
    sf::st_make_valid(x = _)
  
  # Get the NAD83 lat/long constraints for the polygons so that we can snag only
  # the header records in the bounding box.
  current_query_parameters <- sf::st_transform(x = polygons,
                                               crs = 4269) |>
    sf::st_bbox(obj = _) |>
    lapply(X = c("x", "y"),
           current_bounding_coordinates = _,
           inequalities = c(">=",
                            "<="),
           FUN = function(X, current_bounding_coordinates, inequalities){
             list(unname(current_bounding_coordinates[paste0(X, "min")]),
                  unname(current_bounding_coordinates[paste0(X, "max")])) |>
               setNames(object = _,
                        nm = inequalities)
           }) |>
    setNames(object = _,
             nm = c("Longitude_NAD83",
                    "Latitude_NAD83"))
  
  if (verbose) {
    message("Fetching the header information from the LDC.")
  }
  headers_df <- fetch_ldc(data_type = "header",
                          query_parameters = current_query_parameters,
                          username = username,
                          api_key_name = api_key_name,
                          verbose = verbose,
                          timeout = timeout,
                          base_url = base_url)
  
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
  if (data_type == "dataHeader") {
    output <- headers_sf[headers_sf$PrimaryKey %in% intersected_primarykeys, ]
    if (!return_spatial) {
      output <- sf::st_drop_geometry(output)
    }
  } else {
    output <- fetch_ldc(data_type = data_type,
                        query_parameters = list("PrimaryKey" = list("=" = intersected_primarykeys)),
                        username = username,
                        api_key_name = api_key_name,
                        timeout = timeout,
                        delay = delay,
                        base_url = base_url,
                        verbose = verbose)
    if (return_spatial) {
      output <- dplyr::inner_join(x = dplyr::select(.data = headers_sf,
                                                    PrimaryKey),
                                  y = output,
                                  by = "PrimaryKey",
                                  relationship = "one-to-many")
    }
  }
  
  output
}

#' Fetching data from the Landscape Data Commons via API query using ecological site IDs
#' @description This is a wrapper for \code{\link[=fetch_ldc]{fetch_ldc()}} which streamlines retrieving data by ecological site IDs. Most tables in the LDC do not include ecological site information and so this function will identify the PrimaryKeys associated with the requested ecological site(s) and retrieve the requested data associated with those PrimaryKeys.
#' 
#' When you already know the associated PrimaryKeys, use \code{\link[=fetch_ldc]{fetch_ldc()}} instead. If you want to retrieve data associated with polygons, use \code{\link[=fetch_ldc_spatial]{fetch_ldc_spatial()}}.
#' @param data_type Character string. The type of data to query. Valid values are: \code{'gap'}, \code{'header'}, \code{'height'}, \code{'lpi'}, \code{'soilstability'}, \code{'speciesinventory'}, \code{'indicators'}, \code{'species'}, \code{'dustdeposition'}, \code{'horizontalflux'}, and \code{'schema'}.
#' @param ecosite_ids Character vector. All the ecological site IDs (e.g. \code{"R036XB006NM"}) to search for. The returned data will consist only of records where the designated ecological site ID matched one of these values, but there may be ecological site IDS that return no records.
#' @param username Optional character string. The username associated with the LDC API account and which has been used to store one or more API keys using (\code{link[=store_api_key]{store_api_key()}}). This is almost certainly an email address. Some data in the Landscape Data Commons are accessible only to users with appropriate credentials. You do not need to supply credentials, but an API request made without them may return fewer or no data. Defaults to \code{NULL}.
#' @param api_key_name Optional character string. The name used to store the API key to submit with the query and which is associated with \code{username}. If this is \code{NULL} only data which do not require special permissions will be returned. Defaults to \code{NULL}.
# #' @param password Optional character string. The password to supply to the Landscape Data Commons API.  Some data in the Landscape Data Commons are accessible only to users with appropriate credentials. You do not need to supply credentials, but an API request made without them may return fewer or no data. This argument will be ignored if \code{username} is \code{NULL}. Defaults to \code{NULL}.
# #' @param key_chunk_size Numeric. The number of keys to send in a single query. Very long queries fail, so the keys may be chunked into smaller queries with the results of all the queries being combined into a single output. Defaults to \code{100}.
#' @param timeout Numeric. The number of seconds to wait for a nonresponse from the API before considering the query to have failed. Defaults to \code{300}.
#' @param take Optional numeric. The number of records to retrieve at a time. This is NOT the total number of records that will be retrieved! Queries that retrieve too many records at once can fail, so this allows the process to retrieve them in smaller chunks. The function will keep requesting records in chunks equal to this number until all matching records have been retrieved. If this value is too large (i.e., much greater than about \code{10000}), the server will likely respond with a 500 error. If \code{NULL} then all records will be retrieved in a single pass. Defaults to \code{NULL}.
#' @param delay Optional numeric. The number of milliseconds to wait between API queries. Querying too quickly can crash an API or get you locked out, so adjust this as needed. Defaults to \code{500}.
# #' @param exact_match Logical. If \code{TRUE} then only records for which the provided keys are an exact match will be returned. If \code{FALSE} then records containing (but not necessarily matching exactly) the first provided key value will be returned e.g. searching with \code{exact_match = FALSE}, \code{keys = "42"}, and \code{key_type = "EcologicalSiteID"} would return all records in which the ecological site ID contained the string \code{"42"} such as \code{"R042XB012NM"} or \code{"R036XB042NM"}. If \code{FALSE} only the first provided key value will be considered. Using non-exact matching will dramatically increase server response times, so use with caution. Defaults to \code{TRUE}.
#' @param verbose Logical. If \code{TRUE} then the function will report additional diagnostic messages as it executes. Defaults to \code{FALSE}.
#' @returns A data frame of records from the requested \code{data_type} which contain the values from \code{keys} in the variable \code{key_type}.
#' @seealso
#' \code{\link[=fetch_ldc]{fetch_ldc()}} will query for data by any key values.
#' \code{\link[=fetch_ldc_spatial]{fetch_ldc_spatial()}} will query for data by spatial location.
#' @examples
#' # To retrieve all LPI records associated with the ecological sites R036XB006NM and R036XB007NM
#' fetch_ldc_ecosite(data_type = "lpi",
#'                   ecosite_ids = c("R036XB006NM",
#'                                   "R036XB007NM"))
#' @export
fetch_ldc_ecosite <- function(data_type,
                              ecosite_ids,
                              username = NULL,
                              api_key_name = NULL,
                              token = FALSE,
                              timeout = 300,
                              take = NULL,
                              delay = 500,
                              base_url = "https://api.landscapedatacommons.org/api/v1/",
                              verbose = FALSE,
                              keys = deprecated(),
                              key_chunk_size = deprecated(),
                              exact_match = deprecated()) {
  if (lifecycle::is_present(keys)) {
    lifecycle::deprecate_warn(when = "2.0.0",
                              what = "fetch_ldc_ecosite(keys)",
                              with = "fetch_ldc_ecosite(ecosite_ids)")
  }
  
  # This is a super-simple wrapper, but all we're doing is letting the user
  # remain blissfully ignorant while we use fetch_ldc() with multi_table_queries
  # set to TRUE.
  fetch_ldc(query_parameters = list("EcologicalSiteID" = list("=" = ecosite_ids)),
            data_type = data_type,
            username = username,
            api_key_name = api_key_name,
            token = token,
            timeout = timeout,
            take = take,
            delay = delay,
            multi_table_queries = TRUE,
            base_url = base_url,
            verbose = verbose)
}

#' Fetching data from the Landscape Data Commons via API query
#' @description
#' A wrapper for (\code{\link[=fetch_ldc]{fetch_ldc()}}) which will return metadata for the specified table.
#' @param data_type Character string. The type of data to query. To see all valid values, use \code{ldc_table_names()}.
#' @param timeout Numeric. The number of seconds to wait for a nonresponse from the API before considering the query to have failed. Defaults to \code{300}.
#' @param delay Optional numeric. The number of milliseconds to wait between API queries. Querying too quickly can cause issues, so adjust this as needed. Defaults to \code{500}.
#' @param base_url Character string. The URL for the API endpoint to use. Defaults to \code{"https://api.landscapedatacommons.org/api/v1/"}.
#' @param verbose Logical. If \code{TRUE} then the function will report additional diagnostic messages as it executes. Defaults to \code{FALSE}.
#' @returns A data frame of metadata for the requested table. The data frame will contain all variables returned from the server plus an additional character variable called \code{"data_class_r"} which contains the R equivalent to the class of the variable called \code{"data_type"}.
#' @export
fetch_ldc_metadata <- function(data_type,
                               timeout = 300,
                               delay = 500,
                               base_url = "https://api.landscapedatacommons.org/api/v1",
                               verbose = FALSE) {
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  # base_url <- "https://api.landscapedatacommons.org/api/v1/tblSchemaplan?table_name="
  
  # Get the API-recognized name for the submitted alias
  current_table <- ldc_table_names(alias = data_type)
  
  
  if (delay < 0) {
    stop("delay must be a positive numeric value.")
  } else {
    # Convert the value from milliseconds to nanoseconds because we'll be using
    # microbenchmark::get_nanotime() which returns the current time in nanoseconds
    delay <- delay * 10^6
  }
  
  # Build the query by slapping the table name on the end of the base URL.
  current_query <- paste0(base_url,
                          "/tblSchemaplan?table_name=",
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
#' @description Sometimes the data retrieved from the Landscape Data Commons is
#' all character strings even though some variables should at least be numeric.
#' This will coerce the variables into the correct format either using the
#' metadata schema available through the Landscape Data Commons API or by simply
#' attempting to coerce everything to numeric.
#' @param data Data frame. The data to be coerced. This is often the direct output from \code{fetch_ldc()}.
#' @param data_type Character string. If this is a character string recognized by \code{fetch_ldc()} and \code{fetch_ldc_metadata()} then the schema will be retrieved from the LDC and used to coerce values. If this is \code{NULL} then any variable that can be coerced into numeric without producing NA values will be coerced. To see all valid values, use \code{ldc_table_names()}.
#' @param check_api Logical. If \code{FALSE} the schema available as \code{trex::ldc_schema} will be used. If \code{TRUE} the API will be queried for the current schema. Defaults to \code{FALSE}.
#' @param base_url Character string. The URL for the API endpoint to use if \code{check_api} is \code{TRUE}. Defaults to \code{"https://api.landscapedatacommons.org/api/v1/"}.
#' @param verbose Logical. If \code{TRUE} then the function will report additional diagnostic messages as it executes. Defaults to \code{FALSE}.
#' @returns The original data frame, \code{data}, with variables coerced as possible and necessary.
#' @export
coerce_ldc <- function(data,
                       data_type,
                       check_api = FALSE,
                       base_url = "https://api.landscapedatacommons.org/api/v1",
                       verbose = FALSE) {
  data_type <- ldc_table_names(alias = data_type)
  
  if (!is.null(data_type)) {
    if (check_api) {
      if (verbose) {
        message("Requesting schema from the LDC.")
      }
      
      var_lut <- fetch_ldc_metadata(data_type = data_type,
                                    base_url = base_url,
                                    verbose = verbose) |>
        dplyr::select(.data = _,
                      tidyselect::all_of(x = c("field",
                                               "data_class_r")))
    } else {
      if (verbose) {
        message("Using archived schema from package.")
      }
      
      var_lut <- trex::ldc_schema |>
        dplyr::filter(.data = _,
                      table_name == data_type) |>
        dplyr::select(.data = _,
                      tidyselect::all_of(x = c("field",
                                               "data_class_r")))
    }
    
    
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

#' Submit a POST query to the LDC API
#' @description
#' Given a body string (or parsable list of query parameters) and a target table,
#' return all matching records available with the permissions associated with any
#' provided credentials.
#' 
#' This is the function used by all functions with names that start with "fetch_ldc".
#' 
#' @param data_type Character string. The type of data to query. To see all valid values, use \code{ldc_table_names()}.
#' @param body Optional list or character string. This is passed to (\code{link[=format_query_parameters]{format_query_parameters()}}) before being included as the body of the POST submission to the API. If this is \code{NULL} then query will return all available records. Defaults to \code{NULL}.
#' @param api_key OPTIONAL character string. The API key to submit with the request. It is very strongly recommended that you never, ever use your plaintext API key as the value here and instead use (\code{link[=get_stored_key]{get_stored_key()}}) to pull a previously-stored, encrypted key. If \code{NULL} then no key will be used and only data which do not require special permissions will be returned. Defaults to \code{NULL}.
#' @param timeout Numeric. The number of seconds to wait for a nonresponse from the API before considering the query to have failed. Defaults to \code{300}.
#' @param base_url Character string. The URL for the API endpoint to use. Defaults to \code{"https://api.landscapedatacommons.org/api/v1/"}.
#' @param verbose Logical. If \code{TRUE} then the function will report additional diagnostic messages as it executes. Defaults to \code{FALSE}.
#' @export
#' @returns A data frame containing the response from the API.
#' @seealso [fetch_ldc()], [fetch_ldc_spatial()], [fetch_ldc_ecosite()], and [fetch_ldc_metadata()]
query_ldc <- function(data_type,
                      body = NULL,
                      api_key = NULL,
                      token = NULL,
                      timeout = 300,
                      base_url = "https://api.landscapedatacommons.org/api/v1/",
                      verbose = FALSE){
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  
  # If there's no body string provided, just grab everything
  if (is.null(body)) {
    if (verbose) {
      message("No body value provided. All available records will be returned.")
    }
    body_string <- "{}"
  } else {
    if (is.character(body)) {
      body_string <- body
    } else if (is.list(body)) {
      if (length(body) < 1) {
        if (verbose) {
          message("The provided body value is an empty list. All available records will be returned.")
        }
        body_string <- "{}"
      } else {
        if (verbose) {
          message("Attempting to convert the provided list of parameters into a character string.")
        }
        body_string <- format_query_parameters(body)
      }
    } else {
      stop("If the argument body is not NULL it must either be a correctly-formatted string or a list that can be converted to a correctly-string with format_query_parameters().")
    }
  }
  if (verbose) {
    message("Querying using the body:")
    message(body_string)
  }
  
  # Full query response using the API key if we've got one.
  if (is.null(api_key) & is.null(token)) {
    if (verbose) {
      message("No API key or token provided. Only publicly accessible data will be returned.")
    }
    response <- httr::POST(url = paste0(base_url,
                                        data_type),
                           # These helper functions will build the header because
                           # httr::POST() takes these unnamed arguments and uses
                           # them in the header by default.
                           httr::timeout(timeout),
                           httr::user_agent(user_agent),
                           httr::content_type_json(),
                           body = body_string,
                           encode = "json")
    
  } else {
    if (!is.null(api_key)) {
      if (verbose) {
        message("Qualifying data available with the permissions associated with the provided API key will be returned.")
      }
      response <- httr::POST(url = paste0(base_url,
                                          data_type),
                             # These helper functions will build the header because
                             # httr::POST() takes these unnamed arguments and uses
                             # them in the header by default.
                             httr::timeout(timeout),
                             httr::user_agent(user_agent),
                             httr::content_type_json(),
                             httr::add_headers(.headers = c("X-API-Key" = api_key)),
                             body = body_string,
                             encode = "json")
    } else if (!is.null(token)) {
      if (verbose) {
        message("Qualifying data available with the permissions associated with the provided API token will be returned.")
      }
      response <- httr::POST(url = paste0(base_url,
                                          data_type),
                             # These helper functions will build the header because
                             # httr::POST() takes these unnamed arguments and uses
                             # them in the header by default.
                             httr::timeout(timeout),
                             httr::user_agent(user_agent),
                             httr::content_type_json(),
                             httr::add_headers(Authorization = paste("Bearer", 
                                                                     token[["IdToken"]])),
                             body = body_string,
                             encode = "json")
    }
    
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
    } else if (response$status_code == 401) {
      stop(paste0("Query failed with status ",
                  response$status_code,
                  " which is probably due to an expired or invalid API key. Double check that your API key is still valid and consider re-setting it using store_api_key() in case there was a typo."))
    } else if (response$status_code == 400) {
      stop(paste0("Query failed with status ",
                  response$status_code,
                  " which is probably due to an incorrectly formatted query. Check the formatting of your query parameters."))
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
  
  if (nrow(content_df) < 1 & is.null(api_key) & verbose) {
    message("No records were returned, but that may be because no API key was provided and any qualifying records require permissions to access.")
  }
  
  content_df
}

#' Create an API-parsable character string for a parameter
#' @description
#' Parameters submitted to the LDC API must be done via specifically-formatted
#' character strings. This will produce a character string for a parameter.
#' @param variable Character string. The name of the variable in the target table which the operator will be applied to.
#' @param operator Character string. The name of the operator recognized by the LDC API or a valid alias, e.g. \code{">="}. The function (\code{\link[=ldc_api_operators]{ldc_api_operators()}}) can be used to see all recognized values for this argument. Defaults to \code{"equals"}.
#' @param values Single character string or numeric value OR a vector of character strings or numeric values. The value or values to apply the operator to. In the case of any operators other than \code{"equals"} or \code{"notequalto"} \(or an equivalent alias\) this must be numeric and a single value rather than a vector.
#' @returns A character string suitable for inclusion in the body of a POST submission to the LDC API.
#' @export
#' @examples
#' # Produce a string that will limit the API's returned results to only records
#' # where ProjectKey is "BLM_AIM" or "NWERN"
#' stringify_query_parameter(variable = "ProjectKey",
#'                           values = c("BLM_AIM", "NWERN"))
#' stringify_query_parameter(variable = "ProjectKey",
#'                           operator = "oneof",
#'                           values = c("BLM_AIM", "NWERN"))
#' stringify_query_parameter(variable = "ProjectKey",
#'                           operator = "in",
#'                           values = c("BLM_AIM", "NWERN"))
#'                           
#' # Produce a string that will limit the API's returned results to only records
#' # where Latitude_NAD83 is greater than or equal to 40
#' stringify_query_parameter(variable = "Latitude_NAD83",
#'                           operator = ">="
#'                           values = 40)
#' stringify_query_parameter(variable = "Latitude_NAD83",
#'                           operator = "gte"
#'                           values = 40)
#' 
stringify_query_parameter <- function(variable,
                                      operator = "equals",
                                      values){
  
  
  if (!is.character(operator) | length(operator) != 1) {
    stop("The operator argument must be a single character string. Use ldc_api_operators() to see valid operators and their recognized aliases.")
  }
  
  # Keeping this separate for a bit so we can provide meaningful error messages.
  recognized_operator <- ldc_api_operators(operator = operator)
  
  if (!(recognized_operator %in% c("e", "ne"))) {
    if (!is.numeric(values) | length(values) != 1) {
      stop(paste0("Because the provided operator is ", operator, " which maps to the API operator ", recognized_operator, " the values argument must be a single numeric value."))
    }
  } else if (!is.numeric(values) & !is.character(values)) {
    stop(paste0("The values argument must be a single value or vector of values, either numeric or character string(s)."))
  }
  
  if (is.character(values)) {
    values_character_vector <- paste0('"', values, '"')
  } else {
    values_character_vector <- as.character(values)
  }
  values_string <- paste(values_character_vector,
                         collapse = ",")
  if (length(values_character_vector) > 1) {
    values_string <- paste0("[", . = values_string, "]")
  }
  
  # In the case that the operator is literally anything other than "equal to" we
  # need to put the operator information into the output string.
  # The API defaults to assuming that it's "equal to", so we can just skip that
  # if it's the case.
  if (recognized_operator != "e") {
    output <- paste0('"', variable, '":{"$', recognized_operator, '":',
                     values_string, "}")
  } else {
    output <- paste0('"', variable, '":',
                     values_string)
  }
  
  output
}


#' Format query parameters for use with the LDC API using POST
#' @description
#' This converts specially-formatted lists (or already correctly-formatted strings) into a single character string suitable for use as body of a POST submission to the LDC API. If a variable appears more than once in the inputs, it will be combined in the output. Currently, this does not support the use of AND or OR with parameters and so all parameters will be assessed using AND. In the case of conflicting parameters (i.e., using "equals" with any other operator for the same variable) impossible operators will be dropped.
#' @param ... The parameters for the query. These can be provided in three different ways:
#' 1) NAMED arguments where the names are the variables in the data table to use for the query parameters and the values of the arguments are named lists. The lists must be formatted so that the names within the list are the operators to use and the values are vectors of the values associated with each operator. For example, \code{"Latitude" = list(">=" = 40, "<" = 50)} will produce an output to request data where the variable Latitude contains a value greater than or equal to 40 and less than 50.
#' 2) A list of named lists formatted as previously described. The names of the lists must be the names of the variables that they apply to, e.g. \code{list("Longitude" = list(">=" = -105, "<=" = -100), "Latitude" = list("gt" = 40, "lt" = 50))}. This is to make it easier to programmatically generate queries.
#' 3) Parameter strings as produced by \code{stringify_query_parameter()}. If using options 1 or 2, those lists will be run through \code{stringify_query_parameter()}.
#' 
#' Valid operators (and therefore names within lists) are:
#' * \code{"!="} to exclude one or more values from the query results, e.g., \code{"Source" = list("!=" = "BLM_AIM")} will prevent records with "BLM_AIM" in the Source variable from being included in query results.
#' * \code{"="} to limit query results to where only the specified value or values occur, e.g. \code{"Source" = list("=" = c("BLM_AIM", "LMF"))} will prevent records with any value other than "BLM_AIM" or "LMF" in the Source variable from being included in query results.
#' * \code{">"} to limit query results to only where values greater than the provided number occur, e.g. \code{"Latitude_NAD83" = list(">" = 40)} will prevent records with any value less than or equal to 40 in the Longitude variable from being included in query results.
#' * \code{">="} to limit query results to only where values greater than or equal to the provided number occur, e.g. \code{"Latitude_NAD83" = list(">=" = 40)} will prevent records with any value less than 40 in the Longitude variable from being included in query results.
#' * \code{"<"} to limit query results to only where values less than the provided number occur, e.g. \code{"Latitude_NAD83" = list(">" = 40)} will prevent records with any value greater than or equal to 40 in the Longitude variable from being included in query results.
#' * \code{"<="} to limit query results to only where values less than the provided number occur, e.g. \code{"Latitude_NAD83" = list(">=" = 40)} will prevent records with any value greater than 40 in the Longitude variable from being included in query results.
#' 
#' See (\code{\link[=ldc_api_operators]{ldc_api_operators()}}) for additional recognized operator aliases.
#' 
#' A list may contain multiple operators. For example, \code{"Longitude_NAD83" = list(">" = -105, "<" = -100)} will limit query results to only records where the Latitude value is between -105 and -100.
#' 
#' @examples
#' # To produce a character string that can be used as the body for a query which
#' # will return only records with Latitude_NAD83 values between 40 and 45 (inclusive)
#' # and Longitude_NAD83 values greater than -115
#' query_parameters <- list("Latitude_NAD83" = list(">=" = 40,
#'                                                  "<=" = 45),
#'                          "Longitude_NAD83" = list(">" = -115))
#' body <- format_query_parameters(query_parameters)
#' 
#' # This is equivalent to above
#' body_alternate <- format_query_parameters("Latitude_NAD83" = list(">=" = 40,
#'                                                  "<=" = 45),
#'                                           "Longitude_NAD83" = list(">" = -115))
#' identical(body, body_alternate)
#' 
#' @returns A character string suitable for use as the body of a POST submission to the LDC API.
#' @export
#' @seealso [ldc_api_operators()], [stringify_query_parameters()] 
format_query_parameters <- function(...){
  parameter_list <- list(...)
  
  # To support either passing in each parameter as its own argument OR passing
  # in a list/vector of the same, we'll unlist if there's no names for
  # parameter_list because passing in a list of parameters should result in
  # parameter_list just being an unnamed list containing that list.
  if (is.null(names(parameter_list))) {
    parameter_list <- unlist(x = parameter_list,
                             recursive = FALSE)
  }
  
  # This is a very quick-and-dirty check that I'm sure will be inadequate in the
  # case that a user doesn't read the documentation or that I've written it
  # poorly
  if (!all(sapply(X = parameter_list, FUN = is.list)) &
      !all(sapply(X = parameter_list, FUN = is.character))) {
    stop("Unrecognized input format. Please see documentation for guidance on formatting.")
  }
  
  # Now we build the strings per parameter (if they aren't already strings)
  if (all(sapply(X = parameter_list, FUN = is.list))) {
    parameter_vector <- lapply(X = seq_len(length(parameter_list)),
                               parameter_list = parameter_list,
                               FUN = function(X, parameter_list){
                                 current_variable <- names(parameter_list)[X]
                                 current_parameters <- parameter_list[[X]]
                                 
                                 sapply(X = seq_len(length(current_parameters)),
                                        current_parameters = current_parameters,
                                        current_variable = current_variable,
                                        FUN = function(X, current_parameters, current_variable){
                                          stringify_query_parameter(variable = current_variable,
                                                                    operator = names(current_parameters)[X],
                                                                    values = current_parameters[[X]])
                                        })
                               }) |>
      unlist()
  } else {
    parameter_vector <- parameter_list
  }
  
  # This bit figures out the contents of the strings and combines them
  # appropriately. In the interest of ease-of-maintenance rather than elegance
  # there's no specialized version of this for non-string inputs above and this
  # relies on turning them into strings first.
  
  if (length(parameter_vector) == 1) {
    combined_parameters <- parameter_vector
  } else {
    # Get the variables that each query parameter references.
    variables <- sapply(X = parameter_vector,
                        # This pattern will extract only the names of variables
                        # from the beginnings of the strings. It assumes that all
                        # variable names consist only of alphabetic characters,
                        # digits, and underscores. This prevents it from
                        # incorrectly extracting up through any present operators.
                        pattern = '(?<=^")[A-z_\\d]+(?=":)',
                        FUN = stringr::str_extract) |>
      unname()
    
    if (any(is.na(variables))) {
      stop('Unable to identify the variable name for one or more of the supplied parameters. Please check parameter formatting. Keep in mind that the operator must be specified, even when providing specific values to restrict to, e.g., list("ProjectKey" = list("equals" = c("BLM_AIM", "LandPKS")))')
    }
    
    # The goal is to combine parameters where possible, so this identifies the
    # indices corresponding to each unique variable present in the parameters.
    variable_groups <- lapply(X = unique(variables),
                              variables = variables,
                              FUN = function(X, variables){
                                which(variables == X)
                              }) |>
      setNames(object = _,
               nm = unique(variables))
    
    # For each variable, grab the parameters and combine them.
    combined_parameters <- lapply(X = names(variable_groups),
                                  variable_groups = variable_groups,
                                  parameter_vector = parameter_vector,
                                  FUN = function(X, variable_groups, parameter_vector){
                                    current_variable <- X
                                    current_parameters <- unlist(parameter_vector[variable_groups[[current_variable]]])
                                    
                                    operators <- stringr::str_extract(string = current_parameters,
                                                                      pattern = '(?<=")\\$[a-z]+(?=":)') |>
                                      tidyr::replace_na(data = _,
                                                        replace = " ")
                                    
                                    if (any(table(operators) > 1)) {
                                      stop("This package does not currently support building a query that uses an operator more than once for a variable, e.g. expressing '(>= 12 AND < 20) OR (> 50 AND < 80)'. The API does support this, however, and query_ldc() will accept a hand-written query which uses this feature as the body_string argument.")
                                    }
                                    
                                    # parameter_value_strings <- stringr::str_extract_all(string = current_parameters,
                                    #                                                     pattern = "(?<=\\:\\[?).+\\]?\\}?$") |>
                                    parameter_value_strings <- stringr::str_remove_all(string = current_parameters,
                                                                                       pattern = "^.+\\:") |>
                                      stringr::str_remove_all(string = _,
                                                              pattern = "\\}?") |>
                                      unlist() |>
                                      setNames(object = _,
                                               nm = operators)
                                    
                                    # Add brackets back if needed
                                    
                                    nonequal_operators <- setdiff(x = operators,
                                                                  y = " ")
                                    
                                    if (" " %in% operators & length(nonequal_operators) > 0) {
                                      warning(paste0("Because the 'equals' operator is present for the variable ", current_variable,
                                                     " the following operators and their associated values will be dropped: ",
                                                     paste(nonequal_operators,
                                                           collapse = ", "), "."))
                                      operators <- operators[operators %in% c(" ")]
                                      parameter_value_strings <- parameter_value_strings[names(parameter_value_strings) %in% c(" ")]
                                    }
                                    
                                    output <- paste0('"', operators, '":',
                                                     parameter_value_strings) |>
                                      # Because the lack of an operator is parsed
                                      # as "equals" and internally-to-this-function
                                      # handled with a " ", this removes the space
                                      # plus the quotation marks around it and the
                                      # colon following it.
                                      stringr::str_remove_all(string = _,
                                                              pattern = '" ":') |>
                                      paste(. = _,
                                            collapse = ",")
                                    # And because a situation where the user is
                                    # only using "equals" means that there
                                    # shouldn't be {} wrapped around the [],
                                    # we'll just not add those curly braces.
                                    if (identical(operators, c(" "))) {
                                      output <- paste0('"', current_variable, '":',
                                                       output,
                                                       "")
                                    } else {
                                      output <- paste0('"', current_variable, '":{',
                                                       output,
                                                       "}")
                                    }
                                    
                                    output
                                  }) |>
      setNames(object = _,
               nm = names(variable_groups))
  }
  
  
  paste(combined_parameters,
        collapse = ",") |>
    paste0("{",
           . = _,
           "}")
}


#' Look up LDC table names
#' @description
#' Either get a data frame of tables in the LDC which can be queried or get the
#' name of a table in the LDC that corresponds to a recognized human-friendly
#' alias.
#' @param alias Optional character string. A user-supplied name for a data type, e.g. \code{"gap"}. This is case-insensitive and will ignore whitespace and nonalphabetic characters. If \code{NULL} then the lookup table of LDC table names and recognized aliases will be returned instead. Defaults to \code{NULL}.
#' @returns Either a data frame of LDC table names and their recognized aliases OR the name of an LDC table as a character string.
#' @examples
#' # Get the complete lookup table.
#' ldc_table_lookup <- ldc_table_names()
#' 
#' # Get the name of the table associated with gap data.
#' ldc_table_lookup <- ldc_table_names(alias = "gap")
#' 
ldc_table_names <- function(alias = NULL){
  aliases <- list("dataGap" = c("gap", "datagap"),
                  "dataHeader" = c("header", "dataheader"),
                  "dataHeight" = c("height", "heights", "dataheight"),
                  "dataLPI" = c("lpi", "linepoint", "linepointintercept", "datalpi"),
                  "dataSoilStability" = c("soilstability", "datasoilstability"),
                  "dataSpeciesInventory" = c("speciesinventory", "specinv", "dataspeciesinventory"),
                  "geoIndicators" = c("indicators", "geoindicators"),
                  "geoSpecies" = c("species", "geospecies"),
                  "dataAeroSummary" = c("aero", "aerosummary", "dataaerosummary"),
                  "dataPlotCharacterization" = c("plotchar", "plotcharacterization", "dataplotcharacterization"),
                  "dataHorizontalFlux" = c("horizontalflux", "flux", "datahorizontalflux"),
                  "dataSoilHorizons" = c("soil", "soilhorizons", "horizons", "datasoilhorizons"),
                  "tblRHEM" = c("rhem", "tblrhem"),
                  "tblProject" = c("project", "projects", "tblproject"))
  
  if (is.null(alias)) {
    lapply(X = names(aliases),
           aliases = aliases,
           FUN = function(X, aliases){
             data.frame(table = X,
                        alias = aliases[[X]])
           }) |>
      dplyr::bind_rows()
  } else {
    output <- names(aliases)[sapply(X = aliases,
                                    alias = alias,
                                    FUN = function(X, alias){
                                      # Keep only the alphabetic characters
                                      alias <- stringr::str_remove_all(string = alias,
                                                                       pattern = "[^[:alpha:]]") |>
                                        tolower()
                                      alias %in% X
                                    })]
    if (length(output) < 1) {
      stop(paste0(alias, " is not a recognized alias for any table in the LDC. Please use trex::ldc_table_names() to see which aliases are recognized."))
    }
    output
  }
}

#' Look up LDC operators
#' @description
#' Either get a data frame of operators recognized by the LDC API or the operator
#' that corresponds to a recognized human-friendly alias.
#' @param operator Optional character string. A user-supplied name for an operator, e.g. \code{"!="} or \code{"greaterthan"}. This is case-insensitive. If \code{NULL} then the lookup table of recognized operators and their aliases will be returned instead. Defaults to \code{NULL}.
#' @returns Either a data frame of operators and their recognized aliases OR an operator as a character string.
#' @examples
#' # Get the complete lookup table.
#' ldc_api_operator_lookup <- ldc_api_operators()
#' 
#' # Get the operator associated with an alias.
#' ldc_api_operators(operator = "oneof")
#' ldc_api_operators(operator = ">=")
#' 
ldc_api_operators <- function(operator = NULL){
  # This is structured like a list for the convenience of maintenance.
  recognized_operators <- list("gt" = c("gt",
                                        "greaterthan",
                                        "greater",
                                        ">"),
                               "gte" = c("gte",
                                         "greaterthanequal",
                                         "greaterthanorequal",
                                         "greaterorequal",
                                         ">="),
                               "lt" = c("lt",
                                        "lessthan",
                                        "less",
                                        "<"),
                               "lte" = c("lte",
                                         "lessthanequal",
                                         "lessthanorequal",
                                         "lessorequal",
                                         "<="),
                               "ne" = c("ne",
                                        "notequal",
                                        "notequalto",
                                        "doesnotequal",
                                        "!="),
                               "e" = c("e",
                                       "equals",
                                       "equalto",
                                       "in",
                                       "oneof",
                                       "="))
  
  
  if (is.null(operator)) {
    lapply(X = names(recognized_operators),
           recognized_operators = recognized_operators,
           FUN = function(X, recognized_operators){
             data.frame(api_operator = X,
                        alias = recognized_operators[[X]])
           }) |>
      dplyr::bind_rows()
  } else {
    output <- names(recognized_operators)[sapply(X = recognized_operators,
                                                 operator = operator,
                                                 FUN = function(X, operator){
                                                   tolower(operator) %in% X
                                                 })]
    if (length(output) < 1) {
      stop(paste0(operator, " is not a recognized alias for any operator supported by the LDC API. Please use trex::ldc_api_operators() to see which aliases are recognized."))
    }
    output
  }
}