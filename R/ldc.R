# Requires:
# stringr
# httr
# jsonlite

#' Fetching data from the Landscape Data Commons via API query
#' @description A function for making API calls to the Landscape Data Commons based on the table, key variable, and key variable values. It will return a table of records of the requested data type from the LDC in which the variable \code{key_type} contains only values found in \code{keys}. See the \href{https://api.landscapedatacommons.org/api-docs}{API documentation} to see which variables (i.e. \code{key_type} values) are valid for each data type.
#' 
#' There are additional functions to simplify querying by spatial location (\code{\link[=fetch_ldc_spatial]{fetch_ldc_spatial()}}) and by ecological site ID (\code{\link[=fetch_ldc_ecosite]{fetch_ldc_ecosite()}}).
#' @param keys Optional character vector. A character vector of all the values to search for in \code{key_type}. The returned data will consist only of records where \code{key_type} contained one of the key values, but there may be keys that return no records. If \code{NULL} then the entire table will be returned. Defaults to \code{NULL}.
#' @param key_type Optional character string. The name of the variable in the data to search for the values in \code{keys}. This must be the name of a variable that exists in the requested data type's table, e.g. \code{"PrimaryKey"} exists in all tables, but \code{"EcologicalSiteID"} is found only in some. If the function returns a status code of 500 as an error, this variable may not be found in the requested data type. If \code{NULL} then the entire table will be returned. Defaults to \code{NULL}.
#' @param data_type Character string. The type of data to query. Note that the variable specified as \code{key_type} must appear in the table corresponding to \code{data_type}. Valid values are: \code{'gap'}, \code{'header'}, \code{'height'}, \code{'lpi'}, \code{'soilstability'}, \code{'speciesinventory'}, \code{'indicators'}, \code{'species'}, \code{'dustdeposition'}, \code{'horizontalflux'}, and \code{'schema'}.
#' @param key_chunk_size Numeric. The number of keys to send in a single query. Very long queries fail, so the keys may be chunked into smaller queries with the results of all the queries being combined into a single output. Defaults to \code{100}.
#' @param timeout Numeric. The number of seconds to wait for a nonresponse from the API before considering the query to have failed. Defaults to \code{60}.
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
                      key_chunk_size = 100,
                      timeout = 60,
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
                                           "aerosummary"),
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
                                            "aerosummary"))
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
  # results of one query
  data_list <- lapply(X = queries,
                      data_type = data_type,
                      timeout = timeout,
                      take = take,
                      user_agent = user_agent,
                      verbose = verbose,
                      FUN = function(X, data_type, take, timeout, user_agent, verbose){
                        
                        # We handle things differently if the data type is header
                        # because the header table doesn't have an rid variable
                        # and we can't use take or cursor options without that
                        
                        if (data_type == "header" | is.null(take)) {
                          if (verbose) {
                            message("Attempting to query LDC with:")
                            message(X)
                          }
                          
                          # Full query response
                          response <- httr::GET(X,
                                                config = list(httr::timeout(timeout),
                                                              httr::user_agent(user_agent)))
                          
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
                          # Convert from character to data frame
                          content_df <- jsonlite::fromJSON(content_character)
                          
                          content_df
                        } else {
                          # OKAY! So handling using take and cursor options for
                          # anything non-header
                          # The first query needs to not specify the cursor position
                          # and then after that we'll keep trying with the last
                          # rid value + 1 as the cursor until we get an empty
                          # response
                          if (verbose) {
                            message(paste0("Retrieving records in chunks of ", take))
                          }
                          
                          if (grepl(query, pattern = "?")) {
                            query <- paste0(X, "&take=", take)
                          } else {
                            query <- paste0(X, "?take=", take)
                          }
                          
                          
                          if (verbose) {
                            message("Attempting to query LDC with:")
                            message(query)
                          }
                          
                          # Full query response
                          response <- httr::GET(query,
                                                config = list(httr::timeout(timeout),
                                                              httr::user_agent(user_agent)))
                          
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
                          # Convert from character to data frame
                          current_content_df <- jsonlite::fromJSON(content_character)
                          
                          content_df_list <- list(current_content_df)
                          
                          # Here's where we start iterating as long as we're still
                          # getting data
                          # So while the last returned response wasn't empty,
                          # keep requesting the next response where the cursor
                          # is set to the rid following the the highest rid in
                          # the last chunk
                          while (length(content_df_list[[length(content_df_list)]]) > 0) {
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
                            
                            
                            last_rid <- max(content_df_list[[length(content_df_list)]][["rid"]])
                            
                            query <- paste0(X, "&take=", take, "&cursor=", last_rid)
                            
                            if (verbose) {
                              message("Attempting to query LDC with:")
                              message(query)
                            }
                            
                            # Full query response
                            response <- httr::GET(query,
                                                  config = list(httr::timeout(timeout),
                                                                httr::user_agent(user_agent)))
                            
                            # What if there's an error????
                            if (httr::http_error(response)) {
                              stop(paste0("Query failed with status ",
                                          response$status_code))
                            }
                            
                            # Grab only the data portion
                            response_content <- response[["content"]]
                            # Convert from raw to character
                            content_character <- rawToChar(response_content)
                            # Convert from character to data frame
                            current_content_df <- jsonlite::fromJSON(content_character)
                            
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
                          
                          content_df
                        }
                        
                      })
  
  # Combine all the results of the queries
  data <- do.call(rbind,
                  data_list)
  
  # If there aren't data, let the user know
  if (length(data) < 1) {
    warning("No data retrieved. Confirm that your keys and key_type are correct.")
    return(NULL)
  } else {
    # If there are data and the user gave keys, find which if any are missing
    if (!is.null(keys) & exact_match) {
      # Note that we're using keys_vector_original because even if we made
      # alterations to keys_vector, the actual retrieved keys should match the
      # original values despite substituting unicode references for illegal characters
      missing_keys <- keys_vector_original[!(keys_vector_original %in% data[[key_type]])]
      if (length(missing_keys) > 0) {
        warning(paste0("The following keys were not associated with data: ",
                       paste(missing_keys,
                             collapse = ",")))
      }
    }
    return(data)
  }
}

#' Fetching data from the Landscape Data Commons using spatial constraints
#' @description A function for retrieving data from the Landscape Data Commons which fall within a given set of polygons. This is accomplished by retrieving the header information for all points in the LDC, spatializing them, and finding the PrimaryKey values associated with points within the given polygons. Those PrimaryKey values are used to retrieve only the qualifying data from the LDC.
#' 
#' Every time this function is called, it retrieves ALL header information via the API, which can be slow. If you plan to do multiple spatial queries back-to-back, it'll be faster to retrieve the headers with \code{\link[=fetch_ldc]{fetch_ldc()}} once, convert them to an sf object with \code{sf::st_as_sf()}, then use \code{sf:st_intersection()} repeatedly on that sf object to find the PrimaryKey values for each set of polygons and query the API using the PrimaryKeys.
#' 
#' When you already know the associated PrimaryKeys, use \code{\link[=fetch_ldc]{fetch_ldc()}} instead. If you want to retrieve data associated with specific ecological site IDs, use \code{\link[=fetch_ldc_ecosite]{fetch_ldc_ecosite()}}.
#' @param polygons Polygon sf object. The polygon or polygons describing the area to retrieve data from. Only records from sampling locations falling within this area will be returned.
#' @param data_type Character string. The type of data to query. Note that the variable specified as \code{key_type} must appear in the table corresponding to \code{data_type}. Valid values are: \code{'gap'}, \code{'header'}, \code{'height'}, \code{'lpi'}, \code{'soilstability'}, \code{'speciesinventory'}, \code{'indicators'}, \code{'species'}, \code{'dustdeposition'}, \code{'horizontalflux'}, and \code{'schema'}.
#' @param key_chunk_size Numeric. The number of PrimaryKeys to send in a single query. Very long queries fail, so the keys may be chunked into smaller queries with the results of all the queries being combined into a single output. Defaults to \code{100}.
#' @param timeout Numeric. The number of seconds to wait for a nonresponse from the API before considering the query to have failed. Defaults to \code{60}.
#' @param take Optional numeric. The number of records to retrieve at a time. This is NOT the total number of records that will be retrieved! Queries that retrieve too many records at once can fail, so this allows the process to retrieve them in smaller chunks. The function will keep requesting records in chunks equal to this number until all matching records have been retrieved. If this value is too large (i.e., much greater than about \code{10000}), the server will likely respond with a 500 error. If \code{NULL} then all records will be retrieved in a single pass. Defaults to \code{NULL}.
#' @param delay Optional numeric. The number of milliseconds to wait between API queries. Querying too quickly can crash an API or get you locked out, so adjust this as needed. Defaults to \code{500}.
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
                              key_chunk_size = 100,
                              timeout = 60,
                              take = NULL,
                              delay =  500,
                              verbose = FALSE) {
  if (!("sf" %in% class(polygons))) {
    stop("polygons must be a polygon sf object")
  }
  
  # Just to get a unique ID in there for sure without having to ask the user
  polygons$unique_id <- 1:nrow(polygons)
  
  if (verbose) {
    message("Fetching the header information from the LDC.")
  }
  headers_df <- fetch_ldc(data_type = "header")
  
  # We know that the header info includes coordinates in NAD83, so we can easily
  # convert the data frame into an sf object
  if (verbose) {
    message("Converting header information into an sf point object.")
  }
  headers_sf <- sf::st_as_sf(x = headers_df,
                             coords = c("Longitude_NAD83",
                                        "Latitude_NAD83"),
                             crs = "+proj=longlat +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +no_defs +type=crs")
  
  # We're just after the PrimaryKey values here
  if (verbose) {
    message("Finding points that fall within the polygons.")
  }
  header_polygons_intersection <- sf::st_intersection(x = headers_sf[, "PrimaryKey"],
                                                      y = sf::st_transform(polygons[, "unique_id"],
                                                                           crs = sf::st_crs(headers_sf)))
  
  # What if there're no qualifying data????
  if (nrow(header_polygons_intersection) < 1) {
    warning("No data were located within the given polygons.")
    return(NULL)
  }
  
  # If there were points found, snag the PrimaryKey values
  intersected_primarykeys <- unique(header_polygons_intersection$PrimaryKey)
  
  # Grab only the data associated with the PrimaryKey values we've got
  if (data_type == "header") {
    headers_df[headers_df$PrimaryKey %in% intersected_primarykeys, ]
  } else {
    fetch_ldc(keys = intersected_primarykeys,
              key_type = "PrimaryKey",
              data_type = data_type,
              key_chunk_size = key_chunk_size,
              timeout = timeout,
              exact_match = TRUE,
              delay = delay,
              verbose = verbose)
  }
}

#' Fetching data from the Landscape Data Commons via API query using ecological site IDs
#' @description This is a wrapper for \code{\link[=fetch_ldc]{fetch_ldc()}} which streamlines retrieving data by ecological site IDs. Most tables in the LDC do not include ecological site information and so this function will identify the PrimaryKeys associated with the requested ecological site(s) and retrieve the requested data associated with those PrimaryKeys.
#' 
#' When you already know the associated PrimaryKeys, use \code{\link[=fetch_ldc]{fetch_ldc()}} instead. If you want to retrieve data associated with polygons, use \code{\link[=fetch_ldc_spatial]{fetch_ldc_spatial()}}.
#' @param keys Character vector. All the ecological site IDs (e.g. \code{"R036XB006NM"}) to search for. The returned data will consist only of records where the designated ecological site ID matched one of these values, but there may be ecological site IDS that return no records.
#' @param data_type Character string. The type of data to query. Valid values are: \code{'gap'}, \code{'header'}, \code{'height'}, \code{'lpi'}, \code{'soilstability'}, \code{'speciesinventory'}, \code{'indicators'}, \code{'species'}, \code{'dustdeposition'}, \code{'horizontalflux'}, and \code{'schema'}.
#' @param key_chunk_size Numeric. The number of keys to send in a single query. Very long queries fail, so the keys may be chunked into smaller queries with the results of all the queries being combined into a single output. Defaults to \code{100}.
#' @param timeout Numeric. The number of seconds to wait for a nonresponse from the API before considering the query to have failed. Defaults to \code{60}.
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
                              key_chunk_size = 100,
                              timeout = 60,
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
            key_chunk_size = key_chunk_size,
            timeout = timeout,
            take = take,
            delay = delay,
            exact_match = TRUE,
            verbose = verbose)
}

#' A function for coercing data in a data frame into an expected format.
#' @description Sometimes the data retrieved from the Landscape Data Commons is all character strings even though some variables should at least be numeric. This will coerce the variables into the correct format either using the metadata schema available through the Landscape Data Commons API or by simply attempting to coerce everything to numeric.
#' @param data Data frame. The data to be coerced. This is often the direct output from \code{fetch_ldc()}.
# #' @param lookup_table Optional data frame. A lookup table of data formats to coerce the data into. Must contain the variables \code{field_var} and \code{field_var_type} where \code{field_var} contains the names of the variables in \code{data} and \code{field_var_type} contains the corresponding data formats that they should be.
# #' @param field_var
# #' @param field_type_var
#' @param use_schema Logical. If \code{TRUE} then the current metadata schema will be downloaded from the Landscape Data Commons and used to determine which data format every variable should be. If \code{FALSE} then the function will make a best guess at which variables should be numeric and coerce only those. Defaults to \code{FALSE}.
#' @param verbose Logical. If \code{TRUE} then the function will report additional diagnostic messages as it executes. Defaults to \code{FALSE}.
#' @returns The original data frame, \code{data}, either with all variable data types matching the schema from the Landscape Data Commons or with variables that could be coerced to numeric made numeric.
#' @export
coerce_ldc <- function(data,
                       # lookup_table = NULL,
                       # field_var = NULL,
                       # field_type_var = NULL,
                       use_schema = FALSE,
                       verbose = FALSE) {
  if (use_schema) {
    if (verbose) {
      message("Using schema downloaded from the LDC.")
    }
    
    if (verbose) {
      message("Using Field as the field variable and DataType as the data type variable.")
    }
    
    # These are the default names for the variables found in the metadata table
    # accessible through the API
    field_var <- "Field"
    field_type_var <- "DataType"
    
    # Just in case they asked to use the official schema but also supplied a
    # lookup table
    if (!is.null(lookup_table)) {
      warning("Lookup table is being ignored in favor of downloading the current schema.")
    }
    
    # The query for the metadata table is straightforward!
    # Be sure to ask for the latest
    query <- "https://api.landscapedatacommons.org/api/v1/tbl-schema/latest"
    
    if (verbose) {
      message("Attempting to query LDC with:")
      message(query)
    }
    
    # Full query results
    response <- httr::GET(query,
                          config = httr::timeout(60))
    # What if there's an error????
    if (httr::http_error(response)) {
      stop(paste0("Retrieving schema from the API failed with status ",
                  response$status_code))
    }
    # Grab only the data portion
    response_content <- response[["content"]]
    # Convert from raw to character
    content_character <- rawToChar(response_content)
    # Convert from character to data frame
    lookup_table <- jsonlite::fromJSON(content_character)
    if (verbose) {
      message("Schema converted from json to character")
    }
    
    # Now, the lookup table will have data types we don't recognize in R, so it
    # needs its own lookup table. Absurd, but true.
    api_to_r_lookup <- data.frame(api_format = c("TEXT",
                                                 "NUMERIC",
                                                 "INTEGER",
                                                 "BIT",
                                                 "DATE",
                                                 "REAL",
                                                 "DOUBLE PRECISION",
                                                 "POSTGIS.GEOMETRY",
                                                 "TIMESTAMP",
                                                 "VARCHAR"),
                                  r_format = c("character",
                                               "numeric",
                                               "numeric",
                                               "logical",
                                               "date",
                                               "numeric",
                                               "numeric",
                                               "geometry",
                                               "date",
                                               "character"))
    
    lookup_table <- merge(x = lookup_table,
                          y = api_to_r_lookup,
                          by.x = field_type_var,
                          by.y = "api_format",
                          all.x = TRUE)
    
    lookup_table_minimum <- unique(lookup_table[, c(field_var, "r_format")])
    
    if (any(table(lookup_table_minimum[[field_var]]) > 1)) {
      bad_variables <- names(table(lookup_table_minimum[[field_var]]))[table(lookup_table_minimum[[field_var]]) > 1]
      stop("The downloaded schema contains the following variable(s) with multiple data types: ",
           paste(bad_variables,
                 collapse = ", "))
    }
    
    # Now to actually do some coercion
    # We're going to do this variable-by-variable in a for loop because that's easy
    data_coerced <- data
    for (data_var in names(data_coerced)) {
      # Just grab the current variable
      current_vector <- data_coerced[[data_var]]
      # How many of these values are NA?
      incoming_na_count <- sum(is.na(current_vector))
      incoming_na_indices <- which(is.na(current_vector))
      
      expected_format <- lookup_table_minimum$r_format[lookup_table_minimum[[field_var]] == data_var]
      
      # Each of these is the same chunk of code, just slightly tweaked to fit
      # the format
      # I could probably do this elegantly without repeated code chunks, but I
      # haven't so just be careful and be sure to edit all of them!
      switch(expected_format,
             "character" = {
               if (class(current_vector) != "character") {
                 if (verbose) {
                   message(paste0("Attempting to coerce ",
                                  data_var,
                                  " to character."))
                 }
                 current_vector_coerced <- as.character(current_vector)
                 
                 coerced_na_count <- sum(is.na(current_vector_coerced))
                 coerced_na_indices <- which(is.na(current_vector_coerced))
                 
                 # For extra security, we'll check that the NA indices are
                 # identical between the incoming and coerced vectors
                 # That lets us catch if the coercion converted any values to NA
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
                                  class(current_vector),
                                  " to character would produce NA values. The variable will not be coerced."))
                 }
               } else {
                 if (verbose) {
                   message(paste0("The variable ",
                                  data_var,
                                  " does not need to be coerced. Skipping."))
                 }
               }
             },
             "numeric" = {
               if (class(current_vector) != "numeric") {
                 if (verbose) {
                   message(paste0("Attempting to coerce ",
                                  data_var,
                                  " to numeric."))
                 }
                 current_vector_coerced <- as.numeric(current_vector)
                 
                 coerced_na_count <- sum(is.na(current_vector_coerced))
                 coerced_na_indices <- which(is.na(current_vector_coerced))
                 
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
                                  class(current_vector),
                                  " to numeric would produce NA values. The variable will not be coerced."))
                 }
               } else {
                 if (verbose) {
                   message(paste0("The variable ",
                                  data_var,
                                  " does not need to be coerced. Skipping."))
                 }
               }
             },
             "logical" = {
               if (class(current_vector) != "logical") {
                 if (verbose) {
                   message(paste0("Attempting to coerce ",
                                  data_var,
                                  " to logical."))
                 }
                 current_vector_coerced <- as.logical(current_vector)
                 
                 coerced_na_count <- sum(is.na(current_vector_coerced))
                 coerced_na_indices <- which(is.na(current_vector_coerced))
                 
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
                                  class(current_vector),
                                  " to logical would produce NA values. The variable will not be coerced."))
                 }
               } else {
                 if (verbose) {
                   message(paste0("The variable ",
                                  data_var,
                                  " does not need to be coerced. Skipping."))
                 }
               }
             },
             "date" = {
               if (class(current_vector) != "date") {
                 if (verbose) {
                   message(paste0("Attempting to coerce ",
                                  data_var,
                                  " to date."))
                 }
                 current_vector_coerced <- as.Date(current_vector)
                 
                 coerced_na_count <- sum(is.na(current_vector_coerced))
                 coerced_na_indices <- which(is.na(current_vector_coerced))
                 
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
                                  class(current_vector),
                                  " to date would produce NA values. The variable will not be coerced."))
                 }
               } else {
                 if (verbose) {
                   message(paste0("The variable ",
                                  data_var,
                                  " does not need to be coerced. Skipping."))
                 }
               }
             },
             "geometry" = {
               message(paste0("The variable ",
                              data_var,
                              " is flagged as geometry. No coercion necessary."))
             })
    }
    
    if (verbose) {
      message("Coercion complete.")
    }
    
  } else {
    if (verbose) {
      message("Attempting to coerce any variables to numeric possible.")
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