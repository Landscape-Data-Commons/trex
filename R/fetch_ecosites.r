#' Fetch ecological sites for a specified MLRA, or all ecological sites if no MLRA is provided
#' @rdname fetch_ecosites
#' @export fetch_ecosites

fetch_ecosites <- function(mlra = NULL){
  
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
  results[["ecoclasses"]]
 
}

