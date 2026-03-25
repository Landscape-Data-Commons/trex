#' Create a standardized name for a trex keyring based on an API username.
#' @description The format is the provided username stripped of any domain plus "_trex_keyring".
#' @param username Character string. The username associated with the LDC API account. This is almost certainly an email address.
#' @returns A character string matching the standard pattern for a trex keyring name. 
generate_keyring_name <- function(username) {
  keyring_name <- stringr::str_remove(string = username,
                                      pattern = "@.+$") |>
    paste0(.x = _,
           "_trex_keyring")
}

#' Find the keyring associated with the provided username.
#' @description If the system doesn't support multiple keyrings, this returns \code{NULL} which is
#' interpreted by keyring:: functions as the default system keyring.
#' If the system DOES support multiple keyrings, this will check for the
#' standard keyring name produced by \code{generate_keyring_name()} and return that name if the keyring exists or \code{NULL} if it doesn't.
#' @param username Character string. The username associated with the LDC API account. This is almost certainly an email address.
#' @param verbose Logical. If \code{TRUE} the function will produce diagnostic
#'   messages. Defaults to \code{FALSE}.
#' @returns A character string matching the standard pattern for a trex keyring name if that keyring exists OR \code{NULL} in cases where keyring does not exist or the system does not support multiple keyrings.
get_trex_keyring_name <- function(username,
                                  verbose = FALSE) {
  if (!keyring::has_keyring_support()) {
    if (verbose) {
      message(paste0("The system doesn't support multiple keychains. Returning NULL (equivalent to the default keyring name for the system/OS)."))
    }
    NULL
  }
  
  keyring_name <- generate_keyring_name(username = username)
  
  if (keyring_name %in% keyring::keyring_list()$keyring) {
    if (verbose) {
      message(paste0("A keyring called ",
                     keyring_name, " exists."))
    }
    keyring_name
  } else {
    if (verbose) {
      message(paste0("There is no keyring called ",
                     keyring_name, ". Returning NULL (equivalent to the default keyring name for the system/OS)."))
    }
    NULL
  }
}

#' Establish a user-specific trex keyring if possible
#' @description
#' Attempt to create a keyring for the user to store their API keys in.
#' @param verbose Logical. If \code{TRUE} the function will produce diagnostic
#'   messages. Defaults to \code{FALSE}.
setup_keyring <- function(username = NULL,
                          keyring_password = NULL,
                          interactive = TRUE,
                          verbose = FALSE) {
  
  if (is.null(username)) {
    if (interactive) {
      username <- askpass::askpass(prompt = paste0("Please provide the username associated with the LDC API account you will be using:"))
    } else {
      stop("An LDC API username must be provided to set up a keyring for API keys. Either provide the username (typically an email address) as the username argument or set the interactive argument to TRUE to be prompted to enter it.")
    }
  }
  
  keyring_name <- get_trex_keyring_name(username = username,
                                        verbose = verbose)
  
  if (is.null(keyring_name)) {
    if (keyring::has_keyring_support()) {
      keyring_name <- generate_keyring_name(username = username)
      
      if (is.null(keyring_password)) {
        if (interactive) {
          keyring_password <- askpass::askpass(prompt = paste0("Please provide a password to use to access API keys stored in the keyring ",
                                                               keyring_name, " in the future:"))
        } else {
          stop("In order to create a new keyring for trex, a password must be set. Please either set the argument interactive to TRUE (preferred) or provide the password as the argument keyring_password (not recommended).")
        }
      }
      
      if (verbose) {
        message(paste0("Adding a keyring for trex API keys called ",
                       keyring_name, "."))
      }
      
      keyring::keyring_create(keyring = keyring_name,
                              password = keyring_password)
    } else {
      # if (verbose) {
      message("This system does not support multiple keyrings so trex will use the default system/OS keyring.")
      # }
    }
  } else {
    message(paste0("A keyring called ", keyring_name, " already exists and will be used for this username."))
  }
}

#' @param username Character string. The username associated with the API key being stored, typically an email address.
#' @param api_key_name Character string. The name to store the API key under in the keyring. Defaults to \code{"default"}.
#' @param overwrite Logical. If \code{TRUE} then an existing key stored with the name provided as \code{api_key_name} will be overwritten. Defaults to \code{FALSE}.
#' @param 
store_api_key <- function(username,
                          # keyring_password = NULL,
                          # api_key = NULL,
                          api_key_name = "default",
                          # interactive = TRUE,
                          overwrite = FALSE,
                          # sanitize = TRUE,
                          verbose = FALSE) {
  keyring_name <- get_trex_keyring_name(username = username,
                                        verbose = verbose)
  
  if (verbose) {
    if (is.null(keyring_name)) {
      message("Using the system/OS default keyring.")
    } else {
      message(paste0("Using the keyring called ",
                     keyring_name, "."))
    }
  }
  
  keyname_taken <- tryCatch(expr = keyring::key_get(service = api_key_name,
                                                    username = username,
                                                    keyring = keyring_name),
                            error = function(error_message){
                              !stringr::str_detect(string = error_message$message,
                                                   pattern = "Element not found")
                            })
  if (is.character(keyname_taken)) {
    keyname_taken <- TRUE
  }
  
  if (keyname_taken & !overwrite) {
    stop("There is already an API key stored under the name '",
         api_key_name, "'. Either provide a different name or set the argument overwrite to TRUE.")
  } else {
    keyring::key_set(service = api_key_name,
                     keyring = keyring_name,
                     username = username,
                     prompt = "LDC API key:")
    if (verbose) {
      message(paste0("Storing the provided API key with the name ",
                     api_key_name, "."))
    }
  }
}



get_stored_key <- function(username,
                           api_key_name = NULL,
                           keyring_name = NULL,
                           verbose = FALSE) {
  
  if (is.null(api_key_name)) {
    if (verbose) {
      message("Using 'default' as the api_key_name.")
    }
    api_key_name <- "default"
  }
  
  if (is.null(keyring_name)) {
    keyring_name <- get_trex_keyring_name(username = username,
                                          verbose = verbose)
  } else {
    if (verbose) {
      message("Using the provided keyring name '",
              keyring_name, "'.")
    }
  }
  
  if (verbose) {
    message("Getting the API key stored with the name ",
            keyring_name, "'.")
  }
  
  keyring::key_get(service = api_key_name,
                   username = username,
                   keyring = keyring_name)
}