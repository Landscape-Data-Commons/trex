# Fetch ecological sites for a specified MLRA, or all ecological sites if no MLRA is provided
# @rdname fetch_ecosites
# @export fetch_ecosites

fetch_ecosites <- function(mlra = NULL, return_only_id = F){
  
  if(is.null(mlra)){
    query <- paste0("https://edit.jornada.nmsu.edu/services/downloads/esd/class-list.json")
  } else {
    query <- paste0("https://edit.jornada.nmsu.edu/services/downloads/esd/", mlra, "/class-list.json")
  }
  
  full_results <- httr::GET(query, config = httr::timeout(60))
  
  # Grab only the data portion
  results_raw <- full_results[["content"]]
  # Convert from raw to character
  results_character <- rawToChar(results_raw)
  # Convert from character to data frame
  results <- jsonlite::fromJSON(results_character)
  # Discard metadata
  results_dataonly <- results[["ecoclasses"]]
  
  # return either entire table or only id's
  if(!return_only_id){
    out <- results_dataonly
  } else {
    out <- results_dataonly$id
  }
  
  return(out)
}

# fetch_edit_descriptions <- function(keys = NULL,
#                       key_type = NULL,
#                       data_type,
#                       key_chunk_size = 100,
#                       timeout = 60,
#                       verbose = FALSE) {

mlra = c("001X", "002X")
data_type = "all"
key_chunk_size = 100
timeout = 60
verbose = FALSE


  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  
  base_url <- "https://edit.jornada.nmsu.edu/services/descriptions/esd/"
  
  queries <- sapply(mlra, function(mlra) {
    ecoclass <- fetch_ecosites(mlra = mlra, return_only_id = T)
    url <- paste0(base_url, mlra, "/", ecoclass, ".json")
    return(url)
  })
  
  valid_tables <- data.frame(data_type = c("general",
                                           "physiographic",
                                           "climatic",
                                           "water",
                                           "soil",
                                           "ecodynamics",
                                           "interpretations",
                                           "supporting",
                                           "reference"),
                             table_name = c("generalInformation",
                                            "physiographicFeatures",
                                            "climaticFeatures",
                                            "waterFeatures",
                                            "soilFeatures",
                                            "ecologicalDynamics",
                                            "interpretations",
                                            "supportingInformation",
                                            "referenceSheet"))
  if (!(data_type %in% valid_tables$data_type)) {
    stop(paste0("data_type must be one of the following character strings: ",
                paste(valid_tables$data_type,
                      collapse = ", "),
                "."))
  }
  
  current_table <- valid_tables[["table_name"]][valid_tables$data_type == data_type]

  ## only allowing search by mlra at the moment  
  if (!(class(mlra) %in% c("character"))) {
    stop("mlra must be a character string or vector of character strings.")
  }
  # 
  # if (!(class(key_type) %in% c("character", "NULL"))) {
  #   stop("key_type must be a character string or NULL.")
  # }
  
  # if (!is.null(keys) & is.null(key_type)) {
  #   stop("Must provide key_type when providing keys")
  # }
  # 
  # If there are no keys, grab the whole table
  # if (is.null(keys)) {
  #   if (!is.null(key_type)) {
  #     warning("No keys provided, ignoring key_type.")
  #   }
  # #

    # queries <- paste0(base_url,
    #                   current_table)
  # } else {

  

  
  #   # If there are keys, chunk them then build queries
  #   if (verbose) {
  #     message("Grouping keys into chunks for queries.")
  #   }
  #   keys_vector <- unlist(lapply(X = mlra,
  #                                FUN = function(X) {
  #                                  trimws(unlist(stringr::str_split(string = X,
  #                                                                   pattern = ",")))
  #                                }))
  #   
  #   key_chunk_count <- ceiling(length(keys_vector) / key_chunk_size)
  #   
  #   keys_chunks <- sapply(X = 1:key_chunk_count,
  #                         keys_vector = keys_vector,
  #                         key_chunk_size = key_chunk_size,
  #                         key_count = length(keys_vector),
  #                         FUN = function(X, keys_vector, key_chunk_size, key_count) {
  #                           min_index <- max(c(1, (X - 1) * key_chunk_size + 1))
  #                           max_index <- min(c(key_count, X * key_chunk_size))
  #                           indices <- min_index:max_index
  #                           paste(keys_vector[indices],
  #                                 collapse = ",")
  #                         })
  #   
  #   if (verbose) {
  #     message("Building queries.")
  #   }
  #   
  # }
  
  # Use the queries to snag data
  data_list <- 
    sapply(queries, function(q) 
           sapply(q, 
                      timeout = timeout,
                      user_agent = user_agent,
                      FUN = function(X, timeout, user_agent){
                        if (verbose) {
                          message("Attempting to query EDIT with:")
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
                        # Convert from character to list
                        content_list <- jsonlite::fromJSON(content_character)
                        # Select only the desired data type
                        content_type <- content_list[[current_table]]
                        
                        # Convert from list to dataframe
                        
                        
                        content_df <- as.data.frame(t(unlist(content_list)))
                        
                        
                        content_df[[current_table]]
                      }))
  
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
