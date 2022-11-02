# Fetch ecological sites for a specified MLRA, or all ecological sites if no MLRA is provided
# @rdname fetch_ecosites
# @export fetch_ecosites

fetch_edit_ecosites <- function(mlra = NULL, return_only_id = F){
  
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
# 
mlra = c("001X")
nested_list = NULL
timeout = 60
verbose = T

fetch_edit_full_descriptions <- function(
    mlra,
    nested_list = T,
    timeout = 60,
    verbose = FALSE) {
  
  ## only allowing search by mlra at the moment  
  if (!(class(mlra) %in% c("character"))) {
    stop("mlra must be a character string or vector of character strings.")
  }
  
  user_agent <- "http://github.com/Landscape-Data-Commons/trex"
  
  base_url <- "https://edit.jornada.nmsu.edu/services/descriptions/esd/"
  
  # iterate through MLRAs
  full_data_list_all_mlra <- lapply(mlra, function(m){
  
    # fetch all ecosites for that mlra
    ecoclass <- fetch_edit_ecosites(mlra = m, return_only_id = T)
    
    # build queries based on ecosite and mlra
    queries <- paste0(base_url, m, "/", ecoclass, ".json")

    # Make a list of all the data frames for the ecoclasses IDs
    full_data_list <- sapply(queries, function(ql){
      lapply(ql, function(q) {
        if(verbose){
          message("Attempting to query EDIT with:")
          message(q)
        }
        jsonlite::fromJSON(q)
      })
    })
    
    # name the list of data iterated through ecoclass
    names(full_data_list) <- ecoclass
    return(full_data_list)
  })
  
  # name the list of data iterated through mlra
  names(full_data_list_all_mlra) <- mlra

  # If nested_list == F, then concatenate each list element to bring it to a single list
  if(!nested_list & length(mlra) > 1){
    data_list_nonest <- full_data_list_all_mlra[[1]]
    for(i in 2:length(full_data_list_all_mlra)){
      data_list_nonest <- c(data_list_nonest, full_data_list_all_mlra[[i]])
    }
    
    out <- data_list_nonest
  } else {
    out <- full_data_list_all_mlra
  }
  
  return(out)
}
# 
# test1 <- fetch_edit_full_descriptions(mlra = "001X", verbose = T, nested_list = T)
# test2 <- fetch_edit_full_descriptions(mlra = c("001X", "002X"), verbose = T, nested_list = T)
# test3 <- fetch_edit_full_descriptions(mlra = c("001X", "002X"), verbose = T, nested_list = F)
# test4 <- fetch_edit_full_descriptions(mlra = "001X", verbose = T, nested_list = F)

mlra = c("001X")
data_type = "general"
timeout = 60
verbose = T

fetch_edit <- function(mlra,
                       data_type,
                       timeout = 60,
                       verbose = FALSE){
  
  ## only allowing search by mlra at the moment  
  if (!(class(mlra) %in% c("character"))) {
    stop("mlra must be a character string or vector of character strings.")
  }
  
  valid_tables <- data.frame(data_type = c("all",
                                           #"general",
                                           "narratives",
                                           "species",
                                           
                                           "physiographic",
                                           "climatic",
                                           "water",
                                           "soil",
                                           "ecodynamics",
                                           "interpretations",
                                           "supporting",
                                           "reference"),
                             table_name = c("all",
                                            #"generalInformation",
                                            "narratives",
                                            "dominantSpecies"
                                            
                                            "physiographicFeatures",
                                            "climaticFeatures",
                                            "waterFeatures",
                                            "soilFeatures",
                                            "ecologicalynamics",
                                            "interpretations",
                                            "supportingInformation",
                                            "referenceSheet"))
  if (!(data_type %in% valid_tables$data_type)) {
    stop(paste0("data_type must be one of the following character strings: ",
                paste(valid_tables$data_type,
                      collapse = ", "),
                "."))
  }
  
  # fetch the descriptions to parse
  full_descriptions <- fetch_edit_full_descriptions(mlra = mlra, nested_list = F, timeout = timeout, verbose = verbose)
  
  current_table <- valid_tables[["table_name"]][valid_tables$data_type == data_type]

  if(current_table == "all"){
    return(full_descriptions)
  }
  
  
  # # in progress
  # ecoclasses <- sapply(full_descriptions, names)
  # 
  # m <- mlra[1]
  # e <- ecoclasses[1]
  # 
  # sapply(mlra, function(m){
  #   sapply(ecoclasses())
  # })
  # if(current_table == "narratives"){
  #   full_descriptions[[m]][[e]][["generalInformation"]][["narratives"]] %>%
  #     as.data.frame()
  # }
  
}

test1 <- fetch_edit("001X", "all")
test2 <- fetch_edit(c("001X", "002X"), "all", verbose = T)
test3 <- fetch_edit(c("001X", "002X"), "general", verbose = T)
test2$AX001X02X003$generalInformation
