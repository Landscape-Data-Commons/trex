#' Fetch ecological sites for a specified MLRA, or all ecological sites if no MLRA is provided
#' @rdname fetch_ecosites
#' @export fetch_ecosites

fetch_edit_ecosites <- function(mlra = NULL, keys = NULL, key_type = NULL, return_only_id = F, key_chunk_size = 100, timeout = 60, verbose = F){
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
