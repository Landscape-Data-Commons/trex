# Requires:
# stringr
# httr
# jsonlite
# 
# 



#' Fetching data from the Landscape Data Commons via API query
#' @description A function for making API calls to the Landscape Data Commons based on the table, key variable, and key variable values. It will return a table of records of the requested data type from the LDC in which the variable \code{key_type} contains only values found in \code{keys}. See the \href{https://api.landscapedatacommons.org/api-docs}{API documentation} to see which variables (i.e. \code{key_type} values) are valid for each data type.
#' 
#' There are additional functions to simplify querying by spatial location (\code{\link[=fetch_ldc_spatial]{fetch_ldc_spatial()}}) and by ecological site ID (\code{\link[=fetch_ldc_ecosite]{fetch_ldc_ecosite()}}).
#' @param keys Optional character vector. A character vector of all the values to search for in \code{key_type}. The returned data will consist only of records where \code{key_type} contained one of the key values, but there may be keys that return no records. If \code{NULL} then the entire table will be returned. Defaults to \code{NULL}.
#' @param key_type Optional character string. The name of the variable in the data to search for the values in \code{keys}. This must be the name of a variable that exists in the requested data type's table, e.g. \code{"PrimaryKey"} exists in all tables, but \code{"EcologicalSiteID"} is found only in some. If the function returns a status code of 500 as an error, this variable may not be found in the requested data type. If \code{NULL} then the entire table will be returned. Defaults to \code{NULL}.
#' @param data_type Character string. The type of data to query. Note that the variable specified as \code{key_type} must appear in the table corresponding to \code{data_type}. Valid values are: \code{'gap'}, \code{'header'}, \code{'height'}, \code{'lpi'}, \code{'soilstability'}, \code{'speciesinventory'}, \code{'indicators'}, \code{'species'}, \code{'speciesinventory'}, \code{'plotchar'},\code{'aero'}, \code{'rhem'}, and \code{'schema'}.
#' @param username Optional character string. The username to supply to the Landscape Data Commons API. Some data in the Landscape Data Commons are accessible only to users with appropriate credentials. You do not need to supply credentials, but an API request made without them may return fewer or no data. This argument will be ignored if \code{password} is \code{NULL}. Defaults to \code{NULL}.
#' @param password Optional character string. The password to supply to the Landscape Data Commons API.  Some data in the Landscape Data Commons are accessible only to users with appropriate credentials. You do not need to supply credentials, but an API request made without them may return fewer or no data. This argument will be ignored if \code{username} is \code{NULL}. Defaults to \code{NULL}.
# #' @param key_chunk_size Numeric. The number of keys to send in a single query. Very long queries fail, so the keys may be chunked into smaller queries with the results of all the queries being combined into a single output. Defaults to \code{100}.
#' @param timeout Numeric. The number of seconds to wait for a nonresponse from the API before considering the query to have failed. Defaults to \code{300}.
#' @param take Optional numeric. The number of records to retrieve at a time. This is NOT the total number of records that will be retrieved! Queries that retrieve too many records at once can fail, so this allows the process to retrieve them in smaller chunks. The function will keep requesting records in chunks equal to this number until all matching records have been retrieved. If this value is too large (i.e., much greater than about \code{10000}), the server will likely respond with a 500 error. If \code{NULL} then all records will be retrieved in a single pass. Defaults to \code{10000}.
#' @param delay Optional numeric. The number of milliseconds to wait between API queries. Querying too quickly can crash an API or get you locked out, so adjust this as needed. Defaults to \code{2000} (2 seconds).
#' @param exact_match Logical. If \code{TRUE} then only records for which the provided keys are an exact match will be returned. If \code{FALSE} then records containing (but not necessarily matching exactly) the first provided key value will be returned e.g. searching with \code{exact_match = FALSE}, \code{keys = "42"}, and \code{key_type = "EcologicalSiteID"} would return all records in which the ecological site ID contained the string \code{"42"} such as \code{"R042XB012NM"} or \code{"R036XB042NM"}. If \code{FALSE} only the first provided key value will be considered. Using non-exact matching will dramatically increase server response times, so use with caution. Defaults to \code{TRUE}.
#' @param coerce Logical. If \code{TRUE} then the returned values will be coerced into the intended class when they don't match, e.g., if a date variable is a character string instead of a date. Defaults to \code{TRUE}. 
# #' @param verb Character string. The method for submitting an API request, either \code{"POST"} or \code{"GET"}. Defaults to \code{"POST"}.
#' @param base_url Character string. The URL for the API endpoint to use. Defaults to \code{"https://api.landscapedatacommons.org/api/v1/"}.
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
                      query_parameters = list(),
                      data_type,
                      username = NULL,
                      api_key_name = NULL,
                      # key_chunk_size = 100,
                      timeout = 300,
                      take = 10000,
                      delay = 2000,
                      exact_match = TRUE,
                      coerce = TRUE,
                      # verb = "POST",
                      base_url = "https://api.landscapedatacommons.org/api/v1/",
                      verbose = FALSE) {
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  
  if (is.null(username) & !is.null(api_key_name)) {
    warning("Although an api_key_name has been specified, no username has been provided and so no stored API key will be retrieved.")
    api_key_name <- NULL
  }
  
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
                       # "dataAeroSummary" = c("aero", "AERO", "aerosummary", "dataAeroSummary"),
                       "tblAero" = c("aero", "AERO", "tblaero", "tblAero"),
                       "dataPlotCharacterization" = c("plotchar", "plotcharacterization", "dataPlotCharacterization"),
                       "dataHorizontalFlux" = c("horizontalflux", "flux", "dataHorizontalFlux"),
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
  
  # Make sure we've got a query_parameters list even if they're using the stupid
  # old arguments from the dark ages
  if (!any(sapply(X = list(keys, key_type), FUN = is.null))) {
    if (verbose) {
      message("Adding the provided keys to the query_parameters list. Consider skipping the keys and key_type arguments and simply using the query_parameter argument instead.")
    }
    query_parameters[["key_type"]] <- list("=" = keys)
  }
  
  # This uses the metadata to confirm that the query won't return an error due
  # to referencing a variable that doesn't exist in the requested table.
  
  available_variables <- fetch_ldc_metadata(data_type = data_type) |>
    dplyr::pull(.data = _,
                var = "field")
  
  query_variables <- names(query_parameters)
  
  unavailable_variables <- setdiff(x = query_variables,
                                   y = available_variables)
  
  if (length(unavailable_variables) > 0) {
    stop(paste0("The following query variables does not appear in ", current_table,
                ": ", paste(unavailable_variables,
                            collapse = ", "), "\n\nPossible valid key_type values for ", current_table, " are: ",
                paste(available_variables,
                      collapse = ", ")))
  }
  
  if (!is.null(take)) {
    if (!is.numeric(take) | length(take) > 1) {
      stop("take must either be NULL or a single numeric value.")
    }
    if (take > 10000) {
      warning(paste0("The current take value (", take, ") is large enough that there may be errors when retrieving data. Consider setting take to 10000 or less."))
    }
  }
  
  # Add take to the parameters
  query_parameters[["take"]] <- list("=" = take)
  
  if (delay < 0) {
    stop("delay must be a positive numeric value.")
  } else {
    # Convert the value from milliseconds to nanoseconds because we'll be using
    # microbenchmark::get_nanotime() which returns the current time in nanoseconds
    delay <- delay * 10^6
  }
  
  if (verbose & is.null(api_key_name)) {
    message("Retrieving only data which do not require credentials.")
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
  
  keep_querying <- TRUE
  data_list <- list()
  while (keep_querying) {
    if (verbose) {
      message("Submitting query to the API.")
    }
    
    current_data <- query_ldc_post(data_type = data_type,
                                   body_string = format_query_parameters(query_parameters),
                                   api_key = get_stored_key(username = username,
                                                            api_key_name = api_key_name),
                                   base_url = base_url,
                                   timeout = timeout,
                                   verbose = verbose)
    
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
      query_parameters[["cursor"]] <- max(current_data$rid)
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
  data <- dplyr::bind_rows(data_list)
  
  # If there aren't data, let the user know
  if (length(data) < 1) {
    warning("No data retrieved. Confirm that your query parameters are qqorrect and that you've used a valid API key (if the data are not publicly accessible).")
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
                              base_url = "https://api.landscapedatacommons.org/api/v1/",
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
                        base_url = base_url,
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
                              base_url = "https://api.landscapedatacommons.org/api/v1/",
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
                               base_url = base_url,
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
            base_url = base_url,
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
                               base_url = "https://api.landscapedatacommons.org/api/v1/tblSchemaplan?table_name=",
                               verbose = FALSE) {
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  # base_url <- "https://api.landscapedatacommons.org/api/v1/tblSchemaplan?table_name="
  
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
    "dataHorizontalFlux" = c("horizontalflux", "flux", "dataHorizontalFlux"),
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
                       base_url = "https://api.landscapedatacommons.org/api/v1/tblSchemaplan?table_name=",
                       verbose = FALSE) {
  if (!is.null(data_type)) {
    if (verbose) {
      message("Requesting schema from the LDC.")
    }
    
    var_lut <- fetch_ldc_metadata(data_type = data_type,
                                  base_url = base_url,
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

query_ldc_post <- function(data_type,
                           body_string = NULL,
                           api_key = NULL,
                           base_url = "https://api.landscapedatacommons.org/api/v1/",
                           timeout = 300,
                           verbose = FALSE){
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  
  # If there's no body string provided,
  if (is.null(body_string)) {
    if (verbose) {
      message("No body_string value provided.")
    }
    body_string <- "{}"
  }
  if (verbose) {
    message("Querying using the body:")
    message(body_string)
  }
  
  # Full query response using the API key if we've got one.
  if (is.null(api_key)) {
    if (verbose) {
      message("No API key provided. Only publicly accessible data will be returned.")
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


stringify_query_parameter <- function(variable,
                                      operator = "equals",
                                      values){
  # This is structured like a list for the convenience of maintenance. Although
  # it's inefficient, the next step is to convert it into a data frame because
  # that's easier to use and it's still computationally very cheap.
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
  recognized_operators <- lapply(X = names(recognized_operators),
                                 recognized_operators = recognized_operators,
                                 FUN = function(X, recognized_operators){
                                   data.frame(operator = X,
                                              aliases = recognized_operators[[X]])
                                 }) |>
    dplyr::bind_rows()
  
  if (!is.character(operator) | length(operator) != 1) {
    stop(paste0("The operator argument must be a single character string. See documentation for valid operators, which include: ",
                paste(unique(recognized_operators$operator),
                      collapse = ", "), "."))
  }
  
  if (!(operator %in% recognized_operators$aliases)) {
    stop(paste0("The current operator ", operator, " is not recognized. See documentation for valid operators, which include: ",
                paste(unique(recognized_operators$operator),
                      collapse = ", "), "."))
  }
  
  if (!(operator %in% recognized_operators$aliases[recognized_operators$operator %in% c("e", "ne")])) {
    if (!is.numeric(values) | length(values) != 1) {
      stop(paste0("Because the provided operator is ", operator, " the values argument must be a single numeric value."))
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
  
  # Get the proper operator for the API
  operator <- recognized_operators$operator[recognized_operators$aliases == operator]
  
  # In the case that the operator is literally anything other than "equal to" we
  # need to put the operator information into the output string.
  # The API defaults to assuming that it's "equal to", so we can just skip that
  # if it's the case.
  if (operator != "e") {
    output <- paste0('"', variable, '":{"$', operator, '":',
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
#' * \code{">"} to limit query results to only where values greater than the provided number occur, e.g. \code{"Longitude" = list(">" = 40)} will prevent records with any value less than or equal to 40 in the Longitude variable from being included in query results.
#' * \code{">="} to limit query results to only where values greater than or equal to the provided number occur, e.g. \code{"Longitude" = list(">=" = 40)} will prevent records with any value less than 40 in the Longitude variable from being included in query results.
#' * \code{"<"} to limit query results to only where values less than the provided number occur, e.g. \code{"Longitude" = list(">" = 40)} will prevent records with any value greater than or equal to 40 in the Longitude variable from being included in query results.
#' * \code{"<="} to limit query results to only where values less than the provided number occur, e.g. \code{"Longitude" = list(">=" = 40)} will prevent records with any value greater than 40 in the Longitude variable from being included in query results.
#' 
#' A list may contain multiple operators, e.g., \code{"Latitude" = list(">" = -105, "<" = -100)} will limit query results to only records where the Latitude value is between -105 and -100.
#' 
#' @returns A character string suitable for use as the body of a POST submission to the LDC API.
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
                                      stop("This package does not currently support building a query that uses an operator more than once for a variable, e.g. expressing '(>= 12 AND < 20) OR (> 50 AND < 80)'. The API does support this, however, and query_ldc() will accept a hand-written query which uses this feature.")
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
                       "dataHorizontalFlux" = c("horizontalflux", "flux", "dataHorizontalFlux"),
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
