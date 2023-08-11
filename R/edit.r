# Requires:
# stringr
# httr
# jsonlite
# dplyr

#' Fetch EDIT data 
#' @description Fetch EDIT data
#' @param mlra Character string, vector of character strings, or in rare 
#' instances NULL. The Major Land Resource Area (MLRA) or MLRAs to query. Only 
#' records from these MLRAs will be returned. Exact MLRA codes are required, to 
#' see a list of all MLRAs and ecological sites use mlra = NULL and data_type = 
#' 'mlra'.
#' @param data_type Restricted character string. The shorthand name of the type 
#' of data to retrieve. Must be one of: 'mlra', 'ecosites', 'climate', 'landforms', 
#' 'physiography interval', 'physiography nominal', 'physiography ordinal', 
#' 'annual production', 'overstory', 'understory', 'rangeland', 'surface cover', 
#' 'parent material', 'soil interval', 'soil nominal', 'soil ordinal', 
#' 'soil profile', 'texture', 'state narratives', or 'transition narratives'.
#' @param tall Optional logical. The function will output tall data if \code{TRUE}, 
#' otherwise data will be in wide format. Defaults to \code{TRUE}. 
#' @param keys Optional character vector. A character vector of all the values 
#' to search for in \code{key_type}. The returned data will consist only of 
#' records where \code{key_type} contained one of the key values, but there may 
#' be keys that return no records. If \code{NULL} then the entire table will be 
#' returned. Defaults to \code{NULL}.
#' @param key_type Optional character string. Variable to query using 
#' \code{keys}. Must be of of: 'precipitation', 'frostFreeDays', 'elevation', 
#' 'slope', 'landform', 'parentMaterialOrigin', parentMaterialKind, or 'surfaceTexture.'
#' Defaults to \code{NULL}.
#' @param key_chunk_size Optional numeric. The number of keys to send in a single query. 
#' Very long queries fail, so the keys may be chunked into smaller queries with 
#' the results of all the queries being combined into a single output. Defaults 
#' to \code{100}.
#' @param timeout Optional numeric. The number of seconds to wait for a nonresponse from 
#' the API before considering the query to have failed. Defaults to \code{60}.
#' @param delay Optional numeric. The number of milliseconds to wait between 
#' API queries. Querying too quickly can crash an API or get you locked out, so 
#' adjust this as needed. Defaults to \code{500}.
#' @param path_unparsable_data Optional character string. Because delimiters are 
#' sometimes present in narrative data, data from certain ecological sites are 
#' not parsable, and must be excluded from function output. If provided, then 
#' the function will save unparsable data to a folder specified here. Defaults 
#' to \code{NULL}, meaning unparsable data is not saved. 
#' @param verbose Optional logical. If \code{TRUE} then the function will report 
#' additional diagnostic messages as it executes. Defaults to \code{FALSE}.

#' @returns A data frame with the requested EDIT data. 
#' 
#' @examples 
#' # To retrieve ecological sites from MLRA 039X 
#' fetch_edit(mlra = "039X", data_type = "ecosites")
#' # To retrieve ecological sites from MLRAs 039X and 040X
#' fetch_edit(mlra = c("039X", "040X"), data_type = "ecosites")
#' # To retrieve climatic feature descriptions from all ecological sites in 
#' # MLRAs 039X and 040X
#' fetch_edit(mlra = c("039X", "040X"), data_type = "climate")
#' # To retrieve climatic feature descriptions from ecological sites that exist 
#' # with slope between 15 and 30%, from MLRAs 039X and 040X.
#' # Note: this includes all sites whose slope range overlaps with the given 
#' # range. For example this will return sites with slope range 25-70%.
#' fetch_edit(mlra = c("039X", "040X"), data_type = "climate", keys = "15:30", key_type = "slope")
#' # Data defaults ot a tall format. To retrieve wide climate data from MLRAs 
#' # 039X and 040X with slope range 15-30%
#' fetch_edit(mlra = c("039X", "040X"), data_type = "rangeland", keys = "15:30", key_type = "slope", tall = FALSE)
#' # MLRA codes must be exact. To see all MLRA codes and names
#' fetch_mlra_codes()
#' 
#' @rdname fetch_edit
#' @export fetch_edit

fetch_edit <- function(mlra,
                       data_type,
                       tall = TRUE,
                       keys = NULL,
                       key_type = NULL,
                       key_chunk_size = 100,
                       timeout = 60,
                       delay = 500,
                       verbose = FALSE,
                       path_unparsable_data = NULL){
  
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  
  # Check data_type
  valid_tables <- data.frame(data_type = c("mlra",
                                           "ecosites",
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
                             table_name = c(NA,
                                            "class-list",
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
  
  if(is.null(mlra) & data_type != "mlra"){
    stop("MLRA is required. To see a lsit of all MLRAs, use mlra = NULL and data_type = 'mlra', or the convenience function fetch_mlra()")
  }
  
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
  
  if (delay < 0) {
    stop("delay must be a positive numeric value.")
  } else {
    # Convert the value from milliseconds to nanoseconds because we'll be using
    # microbenchmark::get_nanotime() which returns the current time in nanoseconds
    delay <- delay * 10^6
  }
  
  # If data_type is mlra and no mlra is provided, fetch all ecosites 
  if(data_type == "mlra" & is.null(mlra)){
    base_url <- "https://edit.jornada.nmsu.edu/services/downloads/esd/geo-unit-list.txt"
    
  } else {
    ecosite_url <- paste0("https://edit.jornada.nmsu.edu/services/downloads/esd/",
                       mlra,
                       "/class-list.txt")
    
    base_url <- paste0("https://edit.jornada.nmsu.edu/services/downloads/esd/",
                       mlra,
                       "/", 
                       current_table,
                       ".txt")  
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
    
    queries <- paste0(base_url)
    queries_ecosites <- paste0(ecosite_url,
                      "?",
                      key_type,
                      "=",
                      keys_chunks)
  }
  
  # Run the queries
  data_list <- lapply(X = queries,
                      timeout = timeout,
                      user_agent = user_agent,
                      delay = delay,
                      verbose = verbose,
                      FUN = edit_query)
  if(!is.null(keys)){
    ecosite_list <- lapply(X = queries_ecosites,
                           timeout = timeout,
                           user_agent = user_agent,
                           delay = delay,
                           verbose = verbose,
                           FUN = edit_query)
    
    # If there are keys, get necosites from the ecosite list. Otherwise from data_list
    necosites <- sum(sapply(ecosite_list, length))
    
    # Turn the list of data frames into one data frame
    results_ecosites <- dplyr::bind_rows(ecosite_list)
    
  } else {
    necosites <- sum(sapply(data_list, length))
  }

  # If no data is present, stop the function
  if(necosites == 0){
    stop("No data returned")
  }
  
  # Turn the list of data frames into one data frame
  results_dataonly <- dplyr::bind_rows(data_list)
  
  # Replace "" with NA
  results_dataonly[results_dataonly == ""] <- NA
  
  # Enforce numeric type
  results_dataonly <- suppressWarnings(
    dplyr::mutate_if(results_dataonly, names(results_dataonly) %in% c(
      "Representative low", 
      "Representative high",
      "Range low", 
      "Range high", 
      "Average", 
      "Production low", 
      "Production RV", 
      "Production high",
      "Foliar cover low", 
      "Foliar cover high",
      "Canopy cover low",
      "Canopy cover high",
      "Canopy bottom height",
      "Canopy top height",
      "Cover low", 
      "Cover high",
      "Top depth", 
      "Bottom depth",
      "Canopy bottom height", 
      "Canopy top height", 
      "Tree diameter low", 
      "Tree diameter high", 
      "Tree basal area low", 
      "Tree basal area high"
    ),
    as.numeric))
  
  ## Filter data by ecological site (queries can only be done on ecosites)
  if(is.null(keys)){
    results_dataonly <- results_dataonly
  } else {
    results_dataonly <- subset(results_dataonly, `Ecological site ID` %in% results_ecosites$`Ecological site ID`)
  }
  
  # Tall output is ready
  if(tall){
    return(results_dataonly)
  } else {
    # Pivot data if tall is FALSE
    if(data_type %in% c("landforms", "ecosites", "parent material", "texture", "state narratives", "transition narratives", "mlra")){ # No pivot needed
      results_pivot <- results_dataonly 
      if(verbose){
        message("No pivot necessary for this data_type. Tall and wide output are identical.")
      }
    } else if(data_type %in% c("climate")){
      # Measurement unit is often left blank
      results_dataonly[results_dataonly$`Measurement unit` == "" | 
                         is.na(results_dataonly$`Measurement unit`) | 
                         is.null(results_dataonly$`Measurement unit`), 
                       "Measurement unit"] <- "unknown unit"
      
      results_pivot <- tidyr::pivot_wider(results_dataonly, 
                                          id_cols = c("MLRA", "Ecological site ID", "Ecological site legacy ID"),
                                          names_from = c("Property", "Measurement unit"), 
                                          values_from = c("Representative low", "Representative high", "Range low", "Range high", "Average"))
      
    } else if(data_type %in% c("physiography interval", "soil interval")){
      results_dataonly[results_dataonly$`Measurement unit` == "" | 
                         is.na(results_dataonly$`Measurement unit`) | 
                         is.null(results_dataonly$`Measurement unit`),
                       "Measurement unit"] <- "unknown unit"
      
      results_pivot <- tidyr::pivot_wider(results_dataonly,
                                          id_cols = c("MLRA", "Ecological site ID", "Ecological site legacy ID"),
                                          names_from = c("Property", "Measurement unit"),
                                          values_from = c("Representative low", "Representative high", "Range low", "Range high"))
      
    } else if(data_type %in% c("physiography ordinal", "soil ordinal")){
      results_pivot <- tidyr::pivot_wider(results_dataonly,
                                          id_cols = c("MLRA", "Ecological site ID", "Ecological site legacy ID"),
                                          names_from = c("Property"),
                                          values_from = c("Representative low", "Representative high", "Range low", "Range high"))
      
    } else if(data_type %in% c("physiography nominal", "soil nominal")){
      results_pivot <- tidyr::pivot_wider(results_dataonly,
                                          id_cols = c("MLRA", "Ecological site ID", "Ecological site legacy ID"),
                                          names_from = c("Property"),
                                          values_from = c("Property value"))
      
    } else if(data_type %in% c("annual production")){
      results_pivot <- tidyr::pivot_wider(results_dataonly,
                                          id_cols = c("MLRA", "Ecological site ID", "Ecological site legacy ID", "Land use", "Ecosystem state", "Plant community"),
                                          names_from = c("Plant type"),
                                          values_from = c("Production low", "Production RV", "Production high"))
      
    } else if(data_type %in% c("rangeland")){
      results_dataonly_trimduplicates <- unique(results_dataonly[,c(
        "MLRA", "Ecological site ID", "Ecological site legacy ID", "Land use", 
        "Ecosystem state", "Plant community", "Custom group number", 
        "Plant symbol", "Production low", "Production high", "Foliar cover low", 
        "Foliar cover high")
      ])
      
      results_pivot <- tidyr::pivot_wider(unique(results_dataonly_trimduplicates),
                                          id_cols = c("MLRA", "Ecological site ID", "Ecological site legacy ID", "Land use", "Ecosystem state", "Plant community", "Custom group number"),
                                          names_from = c("Plant symbol"),
                                          values_from = c("Production low", "Production high", "Foliar cover low", "Foliar cover high"))
      
    } else if(data_type %in% c("understory")){
      results_dataonly_trimduplicates <- unique(results_dataonly[,c(
        "MLRA", "Ecological site ID", "Ecological site legacy ID", "Land use", 
        "Ecosystem state", "Plant community",  
        "Plant symbol", "Canopy cover low", "Canopy cover high", "Canopy bottom height", 
        "Canopy top height")
      ])
      
      results_pivot <- tidyr::pivot_wider(unique(results_dataonly_trimduplicates),
                                          id_cols = c("MLRA", "Ecological site ID", "Ecological site legacy ID", "Land use", "Ecosystem state", "Plant community"),
                                          names_from = c("Plant symbol"),
                                          values_from = c("Canopy cover low", "Canopy cover high", "Canopy bottom height", "Canopy top height"))
      
      
    } else if(data_type %in% c("overstory")){
      results_dataonly_trimduplicates <- unique(results_dataonly[,c(
        "MLRA", "Ecological site ID", "Ecological site legacy ID", "Land use", 
        "Ecosystem state", "Plant community",  
        "Plant symbol", "Canopy cover low", "Canopy cover high", "Canopy bottom height", 
        "Canopy top height", "Tree diameter low", "Tree diameter high", 
        "Tree basal area low", "Tree basal area high")
      ])
      
      results_pivot <- tidyr::pivot_wider(unique(results_dataonly_trimduplicates),
                                          id_cols = c("MLRA", "Ecological site ID", "Ecological site legacy ID", "Land use", "Ecosystem state", "Plant community"),
                                          names_from = c("Plant symbol"),
                                          values_from = c("Canopy cover low", "Canopy cover high", "Canopy bottom height", "Canopy top height", "Tree diameter low", "Tree diameter high", "Tree basal area low", "Tree basal area high"))
      
    } else if(data_type %in% c("surface cover")){
      results_pivot <- tidyr::pivot_wider(unique(results_dataonly),
                                          id_cols = c("MLRA", "Ecological site ID", "Ecological site legacy ID", "Land use", "Ecosystem state", "Plant community"),
                                          names_from = c("Cover type"),
                                          values_from = c("Cover low", "Cover high"))
      
    } else if(data_type %in% c("soil profile")){
      results_dataonly[results_dataonly$`Measurement unit` == "" | 
                         is.na(results_dataonly$`Measurement unit`) | 
                         is.null(results_dataonly$`Measurement unit`),
                       "Measurement unit"] <- "unknown unit"
      
      results_pivot <- tidyr::pivot_wider(unique(results_dataonly),
                                          id_cols = c("MLRA", "Ecological site ID", "Ecological site legacy ID"),
                                          names_from = c("Property", "Measurement unit"),
                                          values_from = c("Top depth", "Bottom depth", "Representative low", "Representative high", "Range low", "Range high"))
      
    }

    return(results_pivot)
  }
}

#' Fetch list of MLRA codes and names
#' @description Fetch list of MLRA codes and names
#' @param verbose Optional logical. If \code{TRUE} then the function will report 
#' additional diagnostic messages as it executes. Defaults to \code{FALSE}.

#' @returns A data frame with MLRA code, name, and ecological site count.

#' @rdname fetch_edit
#' @export fetch_mlra_codes
fetch_mlra_codes <- function(verbose = FALSE){
  # This is a very simple function. But it makes life easier, because this 
  # particular code is not intuitive.
  fetch_edit(mlra = NULL, 
             data_type = "mlra", 
             verbose = verbose)
}

#' EDIT API query function
#' @description EDIT API query function
#' @param query Query to run
#' @param timeout Inherited from fetch_edit
#' @param user_agent Inherited from fetch_edit
#' @param delay Inherited from fetch_edit
#' @param verbose Inherited from fetch_edit

#' @noRd
edit_query <- function(query, timeout, user_agent, delay, verbose){
  if (verbose) {
    message("Attempting to query EDIT with:")
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
  
  # To avoid flooding the API server with requests,
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
    warning(paste("No data returned for query", query))
    return()
  }
  
  # First row contains metadata
  content_split <- content_split[2:length(content_split)]
  
  # Remove new line character (which is retained because of the splitting method)
  content_split <- gsub("\n", "", content_split)
  
  # Clean up quotes
  content_split <- gsub("\\\"", "", content_split)
  
  # Split into rows with the right number of tabs and those without
  content_goodrows <- 
    content_split[sapply(sapply(content_split, 
                                strsplit, 
                                split = "(?<=.)(?=\t)", 
                                perl = T), 
                         length) == nheaders]
  content_badrows <- 
    content_split[sapply(sapply(content_split, 
                                strsplit, 
                                split = "(?<=.)(?=\t)", 
                                perl = T), 
                         length) != nheaders]
  
  # # Process the bad rows and write to a text file
  if(length(content_badrows) > 0){
    ecosites_badrows <- 
      sapply(1:length(strsplit(content_badrows, "\t")), 
             function(n) {
               strsplit(content_badrows, "\t")[[n]][[2]]
             })
    
    ecosites_badrows <- 
      unique(ecosites_badrows[ecosites_badrows != ""])
    
    warning(paste0("Tabs present in data from following ecosites, making data from those ecosites unparsable. \n"), 
            paste(paste(ecosites_badrows, collapse = ", ")), "\n",
            "These data will be excluded from function output. If path_unparsable_data was provided, function will save these data to a text file.")
    if(!is.null(path_unparsable_data)){
      outname_badrows <- 
        file.path(path_unparsable_data, 
                  paste0("unparsable_data_", mlra, "_", data_type, "_", Sys.Date(), ".txt"))
      
      write.table(content_badrows, 
                  outname_badrows, 
                  row.names = FALSE, 
                  col.names = FALSE,
                  quote = FALSE)
      if(verbose){
        message(paste("Saving file", outname_badrows))
      } 
    } else {
      warning("path_unparsable_data not provided, unparsable data will not be saved.")
    }
  }
  
  # Now that we're rid of the rows with tabs in the data, split by tabs
  content_splitfinal <- unlist(strsplit(content_goodrows, 
                                        split = "(?<=.)(?=\t)", 
                                        perl = T))
  
  # Remove tabs (which are retained by the above)
  content_splitfinal <- gsub("\t", "", content_splitfinal)
  
  # Convert string into a data frame
  content_df <- as.data.frame(matrix(content_splitfinal, 
                                     ncol = nheaders, 
                                     byrow = T))
  colnames(content_df) <- headers
  
  return(content_df)
}
