# Requires:
# stringr
# httr
# jsonlite

fetch_ldc <- function(keys = NULL,
                      key_type = NULL,
                      data_type,
                      key_chunk_size = 100,
                      timeout = 60,
                      verbose = FALSE) {
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  base_url <- "https://napi.landscapedatacommons.org/api/v1/"
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
                                           "schema"),
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
                                            "tbl-schema/latest"))
  if (!(data_type %in% valid_tables$data_type)) {
    stop(paste0("data_type must be one of the following character strings: ",
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
    stop("Must provide key_type when providing keys")
  }
  
  # If there are no keys, grab the whole table
  if (is.null(keys)) {
    if (!is.null(key_type)) {
      warning("No keys provided, ignoring key_type.")
    }
    queries <- paste0(base_url,
                      current_table)
  } else {
    # If there are keys, chunk them then build queries
    if (verbose) {
      message("Grouping keys into chunks for queries.")
    }
    keys_vector <- unlist(lapply(X = keys,
                                 FUN = function(X) {
                                   trimws(unlist(stringr::str_split(string = X,
                                                                    pattern = ",")))
                                 }))
    
    key_chunk_count <- ceiling(length(keys_vector) / key_chunk_size)
    
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
      message("Building queries.")
    }
    
    queries <- paste0(base_url,
                      current_table,
                      "?",
                      key_type,
                      "=",
                      keys_chunks)
  }
  
  # Use the queries to snag data
  data_list <- lapply(X = queries,
                      timeout = timeout,
                      user_agent = user_agent,
                      FUN = function(X, timeout, user_agent){
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
                          stop(paste0("Query failed with status ",
                                      response$status_code))
                        }
                        
                        # Grab only the data portion
                        response_content <- response[["content"]]
                        # Convert from raw to character
                        content_character <- rawToChar(response_content)
                        # Convert from character to data frame
                        content_df <- jsonlite::fromJSON(content_character)
                        
                        content_df
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
    if (!is.null(keys)) {
      missing_keys <- keys_vector[!(keys_vector %in% data[[key_type]])]
      if (length(missing_keys) > 0) {
        warning(paste0("The following keys were not associated with data: ",
                       paste(missing_keys,
                             collapse = ",")))
      }
    }
    return(data)
  }
}


coerce <- function(data,
                   lookup_table = NULL,
                   field_var = NULL,
                   field_type_var = NULL,
                   use_schema = FALSE,
                   verbose = FALSE) {
  if (use_schema) {
    if (verbose) {
      message("Using schema downloaded from the LDC.")
    }
    
    if (verbose) {
      message("Using Field as the field variable and DataType as the data type variable.")
    }
    
    field_var <- "Field"
    field_type_var <- "DataType"
    
    if (!is.null(lookup_table)) {
      warning("Lookup table is being ignored in favor of downloading the current schema.")
    }
    query <- "https://napi.landscapedatacommons.org/api/v1/tbl-schema/latest"
    
    if (verbose) {
      message("Attempting to query LDC with:")
      message(query)
    }
    
    # Full query results
    full_results <- httr::GET(query,
                              config = httr::timeout(60))
    # Grab only the data portion
    results_raw <- full_results[["content"]]
    # Convert from raw to character
    results_character <- rawToChar(results_raw)
    # Convert from character to data frame
    lookup_table <- jsonlite::fromJSON(results_character)
    if (verbose) {
      message("Schema converted from json to character")
    }
  }
}