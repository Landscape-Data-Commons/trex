# Requires:
# stringr
# httr
# jsonlite
# dplyr

#' Fetch ecological sites for a specified MLRA, or all ecological sites if no MLRA is provided
#' @description 
#' @param mlra Character string or vector of character strings. The Major Land Resource Area (MLRA) or MLRAs to query. Only records from these MLRAs will be returned.
#' @param keys Optional character vector. A character vector of all the values to search for in \code{key_type}. The returned data will consist only of records where \code{key_type} contained one of the key values, but there may be keys that return no records. If \code{NULL} then the entire table will be returned. Defaults to \code{NULL}.
#' @param key_type Optional character string. Variable to query using \code{keys}. Valid key_types are: precipitation, frostFreeDays, elevation, slope, landform, parentMaterialOrigin, parentMaterialKind, and surfaceTexture . Defaults to \code{NULL}
#' @param key_chunk_size Numeric. The number of keys to send in a single query. Very long queries fail, so the keys may be chunked into smaller queries with the results of all the queries being combined into a single output. Defaults to \code{100}.
#' @param timeout Numeric. The number of seconds to wait for a nonresponse from the API before considering the query to have failed. Defaults to \code{60}.
#' @param verbose Logical. If \code{TRUE} then the function will report additional diagnostic messages as it executes. Defaults to \code{FALSE}.
#' @returns A data frame of ecological site ID records meeting the parameters defined by \code{keys} and \code{key_type}.
#' 
#' @examples 
#' # To retrieve all ecological sites in EDIT
#' fetch_edit_ecosites(mlra = NULL)
#' # To retrieve all ecological sites from MLRA 039X
#' fetch_edit_ecosites(mlra = "039X")
#' # To retrieve all ecological sites from MLRAs 039X and 040X
#' fetch_edit_ecosites(mlra = c("039X", "040X"))
#' # To retrieve ecological sites that exist with slope between 15 and 30%, from MLRA 039X. Note: this includes all sites whose slope range overlaps with the given range. For example this will return sites with slope range 25-70%.
#' fetch_edit_ecosites(mlra = "039X", keys = "15:30", key_type = "slope")
#' # To retrieve ecological sites from hill landforms, from MLRA 039X
#' fetch_edit_ecosites(mlra = "039X", keys = "mountain", key_type = "landform")
#' # To retrieve ecological sites that exist at 4500 to 5000 feet of elevation. Note: this includes all sites whose elevation range overlaps with the given range. For example, this will return sites with elevation range 4600-7500 feet.
# fetch_edit_ecosites(mlra = "039X", keys = "4500:5000", key_type = "elevation")

#' @rdname fetch_edit
#' @export fetch_edit_ecosites
#' 
fetch_edit_ecosites <- function(mlra = NULL, 
                                keys = NULL, 
                                key_type = NULL, 
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
  
  return(results_dataonly)
}

#' Fetch ecosystem state tables
#' @description 
#' @param 
#' @returns
#' @examples 
#' 
#' @rdname fetch_edit
#' @export fetch_edit_states
#'  
fetch_edit_states <- function(mlra,
                              keys = NULL,
                              key_type = NULL,
                              key_chunk_size = 100,
                              timeout = 60,
                              verbose = F){
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  
  # Check input classes
  if (!(class(keys) %in% c("character", "NULL"))) {
    stop("keys must be a character string or vector of character strings or NULL.")
  }
  
  if (!(class(mlra) %in% c("character", "NULL"))) {
    stop("key_type must be a character string or NULL.")
  }
  
  if (!is.null(keys) & is.null(key_type)) {
    stop("Must provide key_type when providing keys")
  }
  
  # Pass key and key_type to fetch_edit_ecosites, and fetch ecosites in the specified mlra
  ecosites_df <- fetch_edit_ecosites(mlra = mlra, keys = keys, key_type = key_type,
                                     key_chunk_size = key_chunk_size, timeout = timeout, verbose = verbose)
  ecosites_df$urlsuffix <- paste0(ecosites_df$geoUnit, "/", ecosites_df$id)
  
  if(length(ecosites_df$id) ==  0) {
    stop(paste0("No ecosites retrived with ", key_type, " ", keys))
  }
  
  queries <- paste0("https://edit.jornada.nmsu.edu/services/models/esd/", 
                    ecosites_df$urlsuffix,
                    "/states.json")
  
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
  # names(data_list) <- ecosites_df$id
  
  # Attach MLRA and Ecosite ID to data
  for (i in 1:length(data_list)){
    data_list[[i]]$id <- ecosites_df$id[i]
    data_list[[i]]$mlra <- ecosites_df$geoUnit[i]
  }
  
  # If there aren't data, let the user know
  data_list_trim <- data_list[!grepl("failed with status", data_list)]
  
  if (length(data_list_trim) < 1) {
    warning("No data retrieved. Confirm that your keys and key_type are correct.")
    return(NULL)
  }
  
  # Combine all the results of the queries
  results_dataonly <- do.call(dplyr::bind_rows, data_list_trim)
  
  # Reorder output so that ecosite ID and mlra are on the far left
  colorder <- c("id", "mlra", colnames(results_dataonly)[!colnames(results_dataonly) %in% c("id", "mlra")])
  results_dataonly <- results_dataonly[,colorder]

  # specify that if no data was retrieved for a given ecosite, there is no state data for that site
  results_dataonly[is.na(results_dataonly$type), "type"] <- "no state or plant community data for this site"
  
  return(results_dataonly)
}

#' Fetch rangeland community composition tables
#' @description 
#' @param mlra Character string or vector of character strings. The Major Land Resource Area (MLRA) or MLRAs to query. Only records from these MLRAs will be returned.
#' @param data_type
#' @param keys Optional character vector. A character vector of all the values to search for in \code{key_type}. The returned data will consist only of records where \code{key_type} contained one of the key values, but there may be keys that return no records. If \code{NULL} then the entire table will be returned. Defaults to \code{NULL}.
#' @param key_type Optional character string. Variable to query using \code{keys}. Valid key_types are: precipitation, frostFreeDays, elevation, slope, landform, parentMaterialOrigin, parentMaterialKind, and surfaceTexture . Defaults to \code{NULL}
#' @param query_ecosite
#' @param ecosystem_state_sequence
#' @param land_use_sequence
#' @param community_sequence
#' @param key_chunk_size Numeric. The number of keys to send in a single query. Very long queries fail, so the keys may be chunked into smaller queries with the results of all the queries being combined into a single output. Defaults to \code{100}.
#' @param timeout Numeric. The number of seconds to wait for a nonresponse from the API before considering the query to have failed. Defaults to \code{60}.
#' @param verbose Logical. If \code{TRUE} then the function will report additional diagnostic messages as it executes. Defaults to \code{FALSE}.
#' @returns 
#' 
#' @examples 
#' 
#' @rdname fetch_edit
#' @export fetch_edit_community
#' 
fetch_edit_community <- function(mlra,
                                 data_type,
                                 keys = NULL,
                                 key_type = NULL,
                                 ecosystem_state_sequence = NULL,
                                 land_use_sequence = NULL,
                                 community_sequence = NULL,
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
  if (!(class(keys) %in% c("character", "NULL"))) {
    stop("keys must be a character string or vector of character strings or NULL.")
  }
  
  if (!(class(mlra) %in% c("character", "NULL"))) {
    stop("key_type must be a character string or NULL.")
  }
  
  if (!is.null(keys) & is.null(key_type)) {
    stop("Must provide key_type when providing keys")
  }
  
  # Get list of ecosites to query
  ecosites_df <- fetch_edit_ecosites(mlra = mlra, keys = keys, key_type = key_type,
                                     key_chunk_size = key_chunk_size, timeout = timeout, verbose = verbose)
  ecosites_df$urlsuffix <- paste0(ecosites_df$geoUnit, "/", ecosites_df$id)
  
  if(length(ecosites_df$id) ==  0) {
    stop(paste0("No ecosites retrived with ", key_type, " ", keys))
  }
  
  # If any of the three sequence variables is NULL (land use, community, and ecosystem state), return results for all existing sequences
  if(any(is.null(land_use_sequence), is.null(ecosystem_state_sequence), is.null(community_sequence))){
    message("One or more of land_use_sequence, ecosystem_state_sequence, or community_sequence is NULL. Querying EDIT to find all existing sequences")
    states <- fetch_edit_states(mlra = mlra, keys = keys, key_type = key_type, key_chunk_size = key_chunk_size, 
                                timeout = timeout, verbose = verbose)
    
    communityparams <- subset(states, !is.na(landUse) & !is.na(state) & !is.na(community))[,1:8]
    
    # If a sequence variable is specified, limit community params to only that subset
    if(!is.null(ecosystem_state_sequence)){
      communityparams <- subset(communityparams, state == ecosystem_state_sequence)
    }
    if(!is.null(community_sequence)){
      communityparams <- subset(communityparams, community == community_sequence)
    }
    if(!is.null(land_use_sequence)){
      communityparams <- subset(communityparams, landUse == land_use_sequence)
    }
    
    base_url <- paste(sep = "/", 
                      "https://edit.jornada.nmsu.edu/services/plant-community-tables/esd",
                      communityparams$mlra,
                      communityparams$id,
                      communityparams$landUse,
                      communityparams$state,
                      communityparams$community,
                      current_table)
  } else {
    base_url <- paste(sep = "/",
                      "https://edit.jornada.nmsu.edu/services/plant-community-tables/esd",
                      ecosites_df$urlsuffix,
                      land_use_sequence, 
                      ecosystem_state_sequence, 
                      community_sequence,
                      current_table
    )
  }
  
  # If querying the ecosite rather than the table, don't make keys_vector
  if(is.null(keys)) {
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
  
  # Drop ecological sites that could not be reached, or those with no data
  data_list_trim <- data_list[!grepl("failed with status", data_list) & 
                                !sapply(data_list, length) == 0]
  
  # If there aren't data, let the user know
  if (length(data_list_trim) < 1) {
    warning("No data retrieved. Confirm that your keys and key_type are correct.")
    return(NULL)
  }
  
  # Attach ecosite to the tables before flattening them
  for (i in 1:length(data_list_trim)){
    data_list_trim[[i]]$id <- communityparams$id[i]
    data_list_trim[[i]]$mlra <- communityparams$mlra[i]
    data_list_trim[[i]]$landUse <- communityparams$landUse[i]
    data_list_trim[[i]]$state <- communityparams$state[i]
    data_list_trim[[i]]$community <- communityparams$community[i]
    data_list_trim[[i]]$ecositeName <- communityparams$narratives$name[i]
  }
  
  # Combine all the results of the queries
  results_dataonly <- do.call(rbind, data_list_trim)
  
  # Clear the row names
  row.names(results_dataonly) <- NULL
  
  # Reorder columns
  colorder <- c("id", "mlra", "landUse", "state", "community", "ecositeName",
                colnames(results_dataonly)[!colnames(results_dataonly) %in% c("id", "mlra", "landUse", "state", "community", "ecositeName")])
  results_dataonly <- results_dataonly[,colorder]
  
  
  return(unique(results_dataonly))
}

#' Fetch EDIT descriptions 
#' @description 
#' @param mlra Character string or vector of character strings. The Major Land Resource Area (MLRA) or MLRAs to query. Only records from these MLRAs will be returned.
#' @param data_type
#' @param keys Optional character vector. A character vector of all the values to search for in \code{key_type}. The returned data will consist only of records where \code{key_type} contained one of the key values, but there may be keys that return no records. If \code{NULL} then the entire table will be returned. Defaults to \code{NULL}.
#' @param key_type Optional character string. Variable to query using \code{keys}. Valid key_types are: precipitation, frostFreeDays, elevation, slope, landform, parentMaterialOrigin, parentMaterialKind, and surfaceTexture . Defaults to \code{NULL}
#' @param key_chunk_size Numeric. The number of keys to send in a single query. Very long queries fail, so the keys may be chunked into smaller queries with the results of all the queries being combined into a single output. Defaults to \code{100}.
#' @param timeout Numeric. The number of seconds to wait for a nonresponse from the API before considering the query to have failed. Defaults to \code{60}.
#' @param verbose Logical. If \code{TRUE} then the function will report additional diagnostic messages as it executes. Defaults to \code{FALSE}.

#' @returns 
#' 
#' @examples 
#' 
#' @rdname fetch_edit
#' @export fetch_edit_description
fetch_edit_description <- function(mlra,
                                   data_type,
                                   keys = NULL,
                                   key_type = NULL,
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
                                           "physiography",
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
  if (!(class(keys) %in% c("character", "NULL"))) {
    stop("keys must be a character string or vector of character strings or NULL.")
  }
  
  if (!(class(mlra) %in% c("character", "NULL"))) {
    stop("key_type must be a character string or NULL.")
  }
  
  if (!is.null(keys) & is.null(key_type)) {
    stop("Must provide key_type when providing keys")
  }
  
  ecosites_df <- fetch_edit_ecosites(mlra = mlra, keys = keys, key_type = key_type,
                                     key_chunk_size = key_chunk_size, timeout = timeout, verbose = verbose)
  
  ecosites_df$urlsuffix <- paste0(ecosites_df$geoUnit, "/", ecosites_df$id)
  
  if(length(ecosites_df$id) ==  0) {
    stop(paste0("No ecosites retrived with ", key_type, " ", keys))
  }
  
  if(current_table == "all"){
    base_url <- paste0("https://edit.jornada.nmsu.edu/services/descriptions/esd/",
                       ecosites_df$urlsuffix,
                       ".json")  
  } else {
    base_url <- paste0("https://edit.jornada.nmsu.edu/services/descriptions/esd/",
                       ecosites_df$urlsuffix,
                       ".json")  
  }
  
  
  if(is.null(keys)) {
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
  
  names(data_list) <- ecosites_df$id
  
  data_list <- data_list[!grepl("failed with status", data_list)]
  
  # If there aren't data, let the user know
  if (length(data_list) < 1) {
    warning("No data retrieved. Confirm that your keys and key_type are correct.")
    return(NULL)
  }
  
  # Process data to return data frames with ecosite as row
  if(data_type == "all"){
    return(data_list)
  } else {
    
    data_list_reshape <- sapply(data_list, function(e){
      d <- as.data.frame(t(unlist(e)))
      return(d)
    })
    
    # some data types require different reshaping
    if(data_type %in% c("water", "ecodynamics", "reference")){
      results_dataonly <- as.data.frame(t(data_list_reshape))
      results_dataonly$id <- rownames(results_dataonly)
      rownames(results_dataonly) <- NULL
    } else {
      # Attach ecosite to the tables before flattening them
      for (i in 1:length(data_list_reshape)){
        data_list_reshape[[i]]$id <- names(data_list_reshape)[i]
      }
      
      # Combine all the results of the queries
      results_dataonly <- do.call(dplyr::bind_rows, data_list_reshape)
    }
    
    # Attach mlra data
    results_dataonly$mlra <- ecosites_df$geoUnit
    
    # Reorder output so that ecosite ID and mlra are on the far left
    colorder <- c("id", "mlra", colnames(results_dataonly)[!colnames(results_dataonly) %in% c("id", "mlra")])
    results_dataonly <- results_dataonly[,colorder]

    return(results_dataonly)
  }
}