# Requires:
# stringr
# httr
# jsonlite
# dplyr

#' Fetch EDIT data 
#' @description Fetch EDIT data
#' @param mlra Character string or vector of character strings. The Major Land Resource Area (MLRA) or MLRAs to query. Only records from these MLRAs will be returned.
#' @param data_type Restricted character string. The shorthand name of the type of data to retrieve. Must be one of climate", "ecodynamics", "general", "interpretations", "physiography", "reference", "soil", "supporting", "water", or "states"
#' @param keys Optional character vector. A character vector of all the values to search for in \code{key_type}. The returned data will consist only of records where \code{key_type} contained one of the key values, but there may be keys that return no records. If \code{NULL} then the entire table will be returned. Defaults to \code{NULL}.
#' @param key_type Optional character string. Variable to query using \code{keys}. Valid key_types are: precipitation, frostFreeDays, elevation, slope, landform, parentMaterialOrigin, parentMaterialKind, and surfaceTexture . Defaults to \code{NULL}
#' @param key_chunk_size Numeric. The number of keys to send in a single query. Very long queries fail, so the keys may be chunked into smaller queries with the results of all the queries being combined into a single output. Defaults to \code{100}.
#' @param timeout Numeric. The number of seconds to wait for a nonresponse from the API before considering the query to have failed. Defaults to \code{60}.
#' @param path_unparsable_data Character string. If provided, then the function will save unparsable data to disk 
#' @param verbose Logical. If \code{TRUE} then the function will report additional diagnostic messages as it executes. Defaults to \code{FALSE}.

#' @returns A data frame with the requested EDIT data. 
#' 
#' @examples 
#' # To retrieve ecological sites from MLRA 039X
#' fetch_edit(mlra = "039X", data_type = "ecosites")
#' # To retrieve ecological sites from MLRAs 039X and 040X
#' fetch_edit(mlra = c("039X", "040X"), data_type = "ecosites")
#' # To retrieve climatic feature descriptions from all ecological sites in MLRAs 039X and 040X
#' fetch_edit(mlra = c("039X", "040X"), data_type = "climate")
#' # To retrieve climatic feature descriptions from ecological sites that exist with slope between 15 and 30%, from MLRAs 039X and 040X.
#' # Note: this includes all sites whose slope range overlaps with the given range. For example this will return sites with slope range 25-70%.
#' fetch_edit(mlra = c("039X", "040X"), data_type = "climate", keys = "15:30", key_type = "slope")
#' 
#' @rdname fetch_edit
#' @export fetch_edit
#' 

fetch_edit <- function(mlra,
                       data_type,
                       keys = NULL,
                       key_type = NULL,
                       key_chunk_size = 100,
                       timeout = 60,
                       verbose = F,
                       path_unparsable_data = NULL){
  
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  
  # Check data_type
  valid_tables <- data.frame(data_type = c("ecosites",
                                           "climate",
                                           "landforms",
                                           "physiography interval",
                                           "physiography nominal",
                                           "physiography ordinal",
                                           "annual production",
                                           "overstory",
                                           "understory",
                                           "rangeland",
                                           "surface cover",
                                           "parent material",
                                           "soil interval",
                                           "soil nominal", 
                                           "soil ordinal",
                                           "soil profile",
                                           "texture",
                                           "state narratives",
                                           "transition narratives"),
                             table_name = c("class-list",
                                            "climatic-features",
                                            "landforms",
                                            "physiographic-interval-properties",
                                            "physiographic-nominal-properties",
                                            "physiographic-ordinal-properties",
                                            "annual-production",
                                            "forest-overstory",
                                            "forest-understory",
                                            "rangeland-plant-composition",
                                            "soil-surface-cover",
                                            "soil-parent-material",
                                            "soil-interval-properties",
                                            "soil-nominal-properties", 
                                            "soil-ordinal-properties",
                                            "soil-profile-properties",
                                            "soil-surface-textures",
                                            "model-state-narratives",
                                            "model-transition-narratives"
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
  
  if(!is.null(key_type)){
    if(key_type == "id"){
      keys <- NULL
      key_type <- NULL
    }
  }
  
  base_url <- paste0("https://edit.jornada.nmsu.edu/services/downloads/esd/",
                     mlra,
                     "/", 
                     current_table,
                     ".txt")  
  
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
  
  # Run the queries
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
                        
                        # Unnecessary special characters cause problems, so get rid of them
                        content_character <- gsub("\r", " ", content_character)
                        
                        # Get headers and count them
                        headers  <- strsplit(strsplit(content_character, "\n")[[1]][3], "\t")[[1]]
                        headers <- gsub("\\\"", "", headers)
                        nheaders  <- length(headers)
                        
                        ## Sometimes there are tabs in the narrative data. Have to make a vector delimited by new line + start of an mlra code,
                        ## then count the number of columns between each new line delimiter, and identify the bad rows
                        
                        # Because we split by detecting new line and start of mlra, need to retain the split
                        content_split <- strsplit(content_character, "(?<=.)(?=\n\\d{3})", perl = T)[[1]]
                        
                        # If this is true, there is no data present. Exit the apply in order to not create a dataframe with a blank row
                        if(length(content_split) ==  1){ 
                          warning(paste("No data returned for query", X))
                          return()
                        }
                        
                        # First row contains metadata
                        content_split <- content_split[2:length(content_split)]
                        
                        # Remove new line character (which is retained because of the splitting method)
                        content_split <- gsub("\n", "", content_split)
                        
                        # Clean up quotes
                        content_split <- gsub("\\\"", "", content_split)
                        
                        # Split into rows with the right number of tabs and those without
                        content_goodrows <- content_split[sapply(sapply(content_split, strsplit, split = "(?<=.)(?=\t)", perl = T), length) == nheaders]
                        content_badrows <- content_split[sapply(sapply(content_split, strsplit, split = "(?<=.)(?=\t)", perl = T), length) != nheaders]
                        
                        # # Process the bad rows and write to a text file
                        if(length(content_badrows) > 0){
                          ecosites_badrows <- sapply(1:length(strsplit(content_badrows, "\t")), function(n) {
                            strsplit(content_badrows, "\t")[[n]][[2]]
                          })
                          
                          ecosites_badrows <- unique(ecosites_badrows[ecosites_badrows != ""])
                          
                          if(length(ecosites_badrows) > 0){
                            warning(paste0("Tabs present in data from following ecosites, making data from those ecosites unparsable. These data will be excluded from function output. \n"), 
                                    paste(ecosites_badrows, collapse = ", "))
                            if(!is.null(path_unparsable_data)){
                              write.table(content_badrows, 
                                          file.path(path_unparsable_data, 
                                                    paste0("unparsable_data_", mlra, "_", Sys.Date(), ".txt")), 
                                          row.names = F, 
                                          col.names = F)
                            } else {
                              warning("path_unparsable_data not provided, unparsable data will not be saved.")
                            }
                          }
                          
                        }
                        
                        
                        # Now that we're rid of the rows with tabs in the data, split by tabs
                        content_splitfinal <- unlist(strsplit(content_goodrows, split = "(?<=.)(?=\t)", perl = T))
                        # content_splitfinal <- unlist(strsplit(content_goodrows, split = "[\t]"))
                        
                        # # Remove tabs (which are retained by the above)
                        content_splitfinal <- gsub("\t", "", content_splitfinal)
                        
                        # Convert string into a data frame
                        content_df <- as.data.frame(matrix(content_splitfinal, ncol = nheaders, byrow = T))
                        colnames(content_df) <- headers
                        
                        # Due to processing everything as a character, string '"NA"' is present and must be removed
                        content_df <- as.data.frame(sapply(content_df, function(r) gsub("^NA$", NA, r)))
                        
                        return(content_df)
                      })
  
  results_dataonly <- do.call(rbind, data_list)
  return(results_dataonly)
}

