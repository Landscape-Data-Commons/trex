#' Fetch ecological sites for a specified MLRA, or all ecological sites if no MLRA is provided
#' @rdname fetch_edit
#' @export fetch_edit_ecosites
fetch_edit_ecosites <- function(mlra = NULL, 
                                keys = NULL, 
                                key_type = NULL, 
                                return_only_id = F, 
                                key_chunk_size = 100, 
                                timeout = 60, 
                                verbose = F){
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  
  # Check input classes
  # There are a limited range of queriable parameters
  valid_key_types <- c("precipitation", 
                       "frostFreeDays", 
                       "elevation", 
                       "slope", 
                       "landform", 
                       "parentMaterialOrigin", 
                       "parentMaterialKind", 
                       "surfaceTexture")
  
  # Check user input
  if (!is.null(key_type)) { 
    if(!key_type %in% valid_key_types)
      stop(paste0("key_type must be one of the following character strings: ",
                  paste(valid_key_types,
                        collapse = ", "),
                  "."))  
  }
  
  if (!(class(keys) %in% c("character", "NULL"))) {
    stop("keys must be a character string or vector of character strings or NULL.")
  }
  
  if (!(class(mlra) %in% c("character", "NULL"))) {
    stop("key_type must be a character string or NULL.")
  }
  
  if (!is.null(keys) & is.null(key_type)) {
    stop("Must provide key_type when providing keys")
  }
  
  # EDIT structure varies if mlra is specified or not
  if(is.null(mlra)){
    base_url <- paste0("https://edit.jornada.nmsu.edu/services/downloads/esd/class-list.json")
  } else {
    base_url <- paste0("https://edit.jornada.nmsu.edu/services/downloads/esd/", mlra, "/class-list.json")
  }
  
  # If there are no keys, grab the whole table
  if (is.null(keys)) {
    if (!is.null(key_type)) {
      warning("No keys provided, ignoring key_type.")
    }
    queries <- base_url
    
  } else {
    # Change spaces in keys to %20
    keys <- gsub(" ", "%20", keys)
    
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
                      "?",
                      key_type,
                      "=",
                      keys_chunks)
  }
  
  data_list <- lapply(X = queries,
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
                        # Convert from character to data frame
                        content_df <- jsonlite::fromJSON(content_character)
                        
                        content_df[["ecoclasses"]]
                      })
  
  # Combine all the results of the queries
  results_dataonly <- do.call(rbind, data_list)
  
  # If there aren't data, let the user know
  if (length(results_dataonly) < 1) {
    warning("No data retrieved. Confirm that your keys and key_type are correct.")
    return(NULL)
  }
  
  # return either entire table or only id's
  if(!return_only_id){
    out <- results_dataonly
  } else {
    out <- results_dataonly$id
  }
  
  return(out)
}

#' Fetch rangeland community composition tables
#' @rdname fetch_edit
#' @export fetch_edit_community
### TO DO : Can you remove the need for the three sequence variables? 
#### Would need to be able to find the entire set of values. Where is that metadata? Could move into a fetch_edit function if so
### The only valid key_type here is measurementSystem. Change this to a single variable with two valid inputs (metric and usc)
### clean up comments and testing code
### add testing code for all parameters
### add the rest of the plant community data types
fetch_edit_community <- function(mlra,
                                 data_type,
                                 keys = NULL,
                                 key_type = NULL,
                                 query_ecosite = TRUE,
                                 ecosystem_state_sequence = 1,
                                 land_use_sequence = 1,
                                 community_sequence = 1,
                                 key_chunk_size = 100,
                                 timeout = 60,
                                 verbose = F){
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  
  # Check data_type
  valid_tables <- data.frame(data_type = c("rangeland",
                                           "overstory",
                                           "understory"),
                             table_name = c("rangeland-plant-composition.json",
                                            "forest-overstory.json",
                                            "forest-understory.json"))
  
  if (!(data_type %in% valid_tables$data_type)) {
    stop(paste0("data_type must be one of the following character strings: ",
                paste(valid_tables$data_type,
                      collapse = ", "),
                "."))
  }
  
  current_table <- valid_tables[["table_name"]][valid_tables$data_type == data_type]
  
  
  # Check input classes
  # There are a limited range of queriable parameters
  if(!query_ecosite) {
    valid_key_types <- c("measurementSystem")
    
    # Check user input
    if (!is.null(key_type)) { 
      if(!key_type %in% valid_key_types)
        stop(paste0("key_type must be one of the following character strings: ",
                    paste(valid_key_types,
                          collapse = ", "),
                    "."))  
    }
  }
  
  if (!(class(keys) %in% c("character", "NULL"))) {
    stop("keys must be a character string or vector of character strings or NULL.")
  }
  
  if (!(class(mlra) %in% c("character", "NULL"))) {
    stop("key_type must be a character string or NULL.")
  }
  
  if (!is.null(keys) & is.null(key_type)) {
    stop("Must provide key_type when providing keys")
  }
  
  # EDIT structure varies if mlra is specified or not
  if(query_ecosite){
    ecosites <- fetch_edit_ecosites(mlra = mlra, keys = keys, key_type = key_type, return_only_id = TRUE,
                                    key_chunk_size = key_chunk_size, timeout = timeout, verbose = verbose)
  } else {
    ecosites <- fetch_edit_ecosites(mlra = mlra, keys = NULL, key_type = NULL, return_only_id = TRUE,
                                    key_chunk_size = key_chunk_size, timeout = timeout, verbose = verbose)
  }
  
  if(length(ecosites) ==  0) {
    stop(paste0("No ecosites retrived with ", key_type, " ", keys))
  }
  
  
  base_url <- paste0(paste0("https://edit.jornada.nmsu.edu/services/plant-community-tables/esd/", mlra), 
                     "/", ecosites, 
                     "/", land_use_sequence, 
                     "/", ecosystem_state_sequence, 
                     "/", community_sequence,
                     "/", current_table
  )
  
  
  
  # If querying the ecosite rather than the table, don't make keys_vector
  
  if (query_ecosite){
    queries <- base_url
  } else if(is.null(keys)) {
    # If there are no keys, grab the whole table
    if (!is.null(key_type)) {
      warning("No keys provided, ignoring key_type.")
    }
    queries <- base_url
    
  } else {
    # Change spaces in keys to %20
    keys <- gsub(" ", "%20", keys)
    
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
                      "?",
                      key_type,
                      "=",
                      keys_chunks)
  }
  
  data_list <- lapply(X = queries,
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
                          warning(paste0(X, " failed with status ",
                                         response$status_code))
                          
                        } else {
                          # Grab only the data portion
                          response_content <- response[["content"]]
                          # Convert from raw to character
                          content_character <- rawToChar(response_content)
                          # Convert from character to data frame
                          content_df <- jsonlite::fromJSON(content_character)
                          
                          return(content_df[[2]])
                        }
                      })
  
  data_list <- data_list[!grepl("failed with status", data_list)]
  
  # Combine all the results of the queries
  results_dataonly <- do.call(rbind, data_list)
  
  # If there aren't data, let the user know
  if (length(results_dataonly) < 1) {
    warning("No data retrieved. Confirm that your keys and key_type are correct.")
    return(NULL)
  }
  
  return(unique(results_dataonly))
}

#' Fetch EDIT descriptions 
#' @rdname fetch_edit
#' @export fetch_edit_description
fetch_edit_description <- function(mlra,
                                   data_type,
                                   keys = NULL,
                                   key_type = NULL,
                                   query_ecosite = TRUE,
                                   key_chunk_size = 100,
                                   timeout = 60,
                                   verbose = F){
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  
  # Check data_type
  valid_tables <- data.frame(data_type = c("all",
                                           "climate",
                                           "ecodynamics",
                                           "general",
                                           "interpretations",
                                           "physiographic",
                                           "reference",
                                           "soil",
                                           "supporting",
                                           
                                           "water"),
                             table_name = c("all",
                                            "climatic-features",
                                            "ecological-dynamics",
                                            "general-information",
                                            "interpretations",
                                            "physiographic-features",
                                            "reference-sheet",
                                            "soil-features",
                                            "supporting-information",
                                            "water-features"
                             ))
  
  if (!(data_type %in% valid_tables$data_type)) {
    stop(paste0("data_type must be one of the following character strings: ",
                paste(valid_tables$data_type,
                      collapse = ", "),
                "."))
  }
  
  current_table <- valid_tables[["table_name"]][valid_tables$data_type == data_type]
  
  
  # Check input classes
  # There are a limited range of queriable parameters
  if(!query_ecosite) {
    valid_key_types <- c("measurementSystem")
    
    # Check user input
    if (!is.null(key_type)) { 
      if(!key_type %in% valid_key_types)
        stop(paste0("key_type must be one of the following character strings: ",
                    paste(valid_key_types,
                          collapse = ", "),
                    "."))  
    }
  }
  
  if (!(class(keys) %in% c("character", "NULL"))) {
    stop("keys must be a character string or vector of character strings or NULL.")
  }
  
  if (!(class(mlra) %in% c("character", "NULL"))) {
    stop("key_type must be a character string or NULL.")
  }
  
  if (!is.null(keys) & is.null(key_type)) {
    stop("Must provide key_type when providing keys")
  }
  
  # EDIT structure varies if mlra is specified or not
  if(query_ecosite){
    ecosites <- fetch_edit_ecosites(mlra = mlra, keys = keys, key_type = key_type, return_only_id = TRUE,
                                    key_chunk_size = key_chunk_size, timeout = timeout, verbose = verbose)
  } else {
    ecosites <- fetch_edit_ecosites(mlra = mlra, keys = NULL, key_type = NULL, return_only_id = TRUE,
                                    key_chunk_size = key_chunk_size, timeout = timeout, verbose = verbose)
  }
  
  if(length(ecosites) ==  0) {
    stop(paste0("No ecosites retrived with ", key_type, " ", keys))
  }
  
  if(current_table == "all"){
    base_url <- paste0(paste0("https://edit.jornada.nmsu.edu/services/descriptions/esd/", mlra), 
                       "/", ecosites,
                       ".json")  
  } else {
    base_url <- paste0(paste0("https://edit.jornada.nmsu.edu/services/descriptions/esd/", mlra), 
                       "/", ecosites, 
                       "/", current_table,
                       ".json")
  }
  
  # If querying the ecosite rather than the table, don't make keys_vector
  if (query_ecosite){
    queries <- base_url
  } else if(is.null(keys)) {
    # If there are no keys, grab the whole table
    if (!is.null(key_type)) {
      warning("No keys provided, ignoring key_type.")
    }
    queries <- base_url
    
  } else {
    # Change spaces in keys to %20
    keys <- gsub(" ", "%20", keys)
    
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
                      "?",
                      key_type,
                      "=",
                      keys_chunks)
  }
  
  data_list <- lapply(X = queries,
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
                          warning(paste0(X, " failed with status ",
                                         response$status_code))
                          
                        } else {
                          # Grab only the data portion
                          response_content <- response[["content"]]
                          # Convert from raw to character
                          content_character <- rawToChar(response_content)
                          # Convert from character to data frame
                          content_df <- jsonlite::fromJSON(content_character)
                          
                          return(content_df[[2]])
                        }
                      })
  
  names(data_list) <- ecosites
  
  data_list <- data_list[!grepl("failed with status", data_list)]
  
  # If there aren't data, let the user know
  if (length(data_list) < 1) {
    warning("No data retrieved. Confirm that your keys and key_type are correct.")
    return(NULL)
  }
  
  return(data_list)
}

test <- fetch_edit_description("001X", "climate", verbose = T)
test$AX001X02X003$narratives

#'  Fetch edit full descriptions
#' @rdname fetch_edit
#' @export fetch_edit_full_descriptions
#### TO DO : move this into a fetch_edit_class
### add class checks for keys and keys type
# fetch_edit_full_descriptions <- function(
#     mlra,
#     keys = NULL,
#     key_type = NULL,
#     nested_list = T,
#     timeout = 60,
#     verbose = FALSE) {
#   user_agent <- "http://github.com/Landscape-Data-Commons/trex"
#   base_url <- "https://edit.jornada.nmsu.edu/services/descriptions/esd/"
#   
#   ## only allowing search by mlra at the moment  
#   if (!(class(mlra) %in% c("character"))) {
#     stop("mlra must be a character string or vector of character strings.")
#   }
#   
#   # iterate through MLRAs
#   full_data_list_all_mlra <- lapply(mlra, function(m){
#     
#     # fetch all ecosites for that mlra
#     ecoclass <- fetch_edit_ecosites(mlra = m, return_only_id = T)
#     
#     # build queries based on ecosite and mlra
#     queries <- paste0(base_url, m, "/", ecoclass, ".json")
#     
#     # Make a list of all the data frames for the ecoclasses IDs
#     full_data_list <- sapply(queries, function(ql){
#       lapply(ql, function(q) {
#         if(verbose){
#           message("Attempting to query EDIT with:")
#           message(q)
#         }
#         jsonlite::fromJSON(q)
#       })
#     })
#     
#     # name the list of data iterated through ecoclass
#     names(full_data_list) <- ecoclass
#     return(full_data_list)
#   })
#   
#   # name the list of data iterated through mlra
#   names(full_data_list_all_mlra) <- mlra
#   
#   # If nested_list == F, then concatenate each list element to bring it to a single list
#   if(!nested_list & length(mlra) > 1){
#     data_list_nonest <- full_data_list_all_mlra[[1]]
#     for(i in 2:length(full_data_list_all_mlra)){
#       data_list_nonest <- c(data_list_nonest, full_data_list_all_mlra[[i]])
#     }
#     
#     out <- data_list_nonest
#   } else {
#     out <- full_data_list_all_mlra
#   }
#   
#   return(out)
# }
# 
