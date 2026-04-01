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
#' Attempt to create a keyring for the user to store their API keys in. The keyring name is created using \code{generate_keyring_name()}.
#' @param username Character string. The username associated with the LDC API account. This is almost certainly an email address.
#' @param keyring_password OPTIONAL (and discouraged) character string. The password to use to encrypt the keyring and which will be used to access and API keys stored in the keyring in the future. It is very strongly recommended that you never save a plaintext password in your code. If this is \code{NULL} and \code{interactive} is \code{TRUE} then you will be prompted to type a password when the function is run. Defaults to \code{NULL}.
#' @param interactive Logical. If \code{TRUE} and either \code{username} or \code{keyring_password} is \code{NULL} then interactive prompts will ask you for the missing values. Defaults to \code{TRUE}.
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
      } else {
        warning("You provided a keyring password as an argument. Make absolutely sure that that password is not saved in plaintext anywhere!")
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

#' Securely store an API key for future use
#' @description
#' Store an API key in the user's encrypted keyring. The user must provide the
#' key when interactively prompted. Other trex functions for requesting data from
#' an API will use their own \code{username} and \code{api_key_name} to access keys
#' stored in this way.
#' 
#' @param username Character string. The username associated with the API key being stored, typically an email address.
#' @param api_key_name Character string. The name to store the API key under in the keyring. Defaults to \code{"default"}.
#' @param overwrite Logical. If \code{TRUE} then an existing key stored with the name provided as \code{api_key_name} will be overwritten. Defaults to \code{FALSE}.
#' @param verbose Logical. If \code{TRUE} the function will produce diagnostic
#'   messages. Defaults to \code{FALSE}.
store_api_key <- function(username,
                          api_key_name = "default",
                          overwrite = FALSE,
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


#' Retrieve a securely-stored API key
#' @description
#' Retrieve a previously-stored API key from the user's encrypted keyring.
#' There is no argument for the keyring password and the user must provide it when interactively prompted when this function is run and the keyring is locked.
#' The keyring is managed by the user's OS and will remain unlocked for a period of time controlled by the OS, so the user will not always be prompted for a password.
#' Other trex functions for requesting data from
#' an API will use their own \code{username} and \code{api_key_name} to access keys
#' stored in this way.
#' 
#' @param username Character string. The username associated with the API key being stored, typically an email address.
#' @param api_key_name Optional character string. The name that the API key was stored under in the keyring. Defaults to \code{NULL} which will attempt to retrieve a key called \code{"default"}.
#' @param keyring_name Optional character string. The name of the keyring that the API key was stored in. If this is \code{NULL} then the keyring matching \code{generate_keyring_name(username)} will be used. Defaults to \code{NULL}.
#' @param accept_failure Logical. If \code{TRUE} then the function will return \code{NULL} if a key can't be found. Otherwise an error message will be produced. Defaults to \code{TRUE}.
#' @param verbose Logical. If \code{TRUE} the function will produce diagnostic
#'   messages. Defaults to \code{FALSE}.
get_stored_key <- function(username,
                           api_key_name = NULL,
                           keyring_name = NULL,
                           accept_failure = TRUE,
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
    message("Getting the API key stored with the name '",
            api_key_name, "'.")
  }
  
  output <- keyring::key_get(service = api_key_name,
                             username = username,
                             keyring = keyring_name) |>
    tryCatch(expr = _,
             error = function(error_message){
               !stringr::str_detect(string = error_message$message,
                                    pattern = "Element not found")
             })
  
  if (!is.character(output)) {
    if (accept_failure & !output) {
      output <- NULL
    } else if (!accept_failure & !output) {
      stop("Unable to find the specified API key.")
    } else if (output) {
      stop("Some kind of error occurred with retrieving the stored API key. Double check your username and api_key_name values.")
    }
  }
  
  output
}

#' Securely store an API key for future use
#' @description
#' Store an API key in the user's encrypted keyring. The user must provide the
#' key when interactively prompted. Other trex functions for requesting data from
#' an API will use their own \code{username} and \code{api_key_name} to access keys
#' stored in this way.
#' 
#' @param username Character string. The username associated with the API key being stored, typically an email address.
#' @param api_key_name Character string. The name to store the API key under in the keyring. Defaults to \code{"default"}.
#' @param overwrite Logical. If \code{TRUE} then an existing key stored with the name provided as \code{api_key_name} will be overwritten. Defaults to \code{FALSE}.
#' @param verbose Logical. If \code{TRUE} the function will produce diagnostic
#'   messages. Defaults to \code{FALSE}.
store_password <- function(username,
                           api_name = "ldc",
                           overwrite = FALSE,
                           verbose = FALSE) {
  api_name <- check_api_name(api_name)
  
  keyring_name <- get_trex_keyring_name(username = username,
                                        verbose = verbose)
  
  service_name <- paste0(api_name,
                         "_password")
  
  if (verbose) {
    if (is.null(keyring_name)) {
      message("Using the system/OS default keyring.")
    } else {
      message(paste0("Using the keyring called ",
                     keyring_name, "."))
    }
  }
  
  keyname_taken <- tryCatch(expr = keyring::key_get(service = service_name,
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
    stop("There is already a password stored for the username '",
         username, "'. Set the argument overwrite to TRUE to replace the stored password.")
  } else {
    keyring::key_set(service = service_name,
                     keyring = keyring_name,
                     username = username,
                     prompt = "LDC API password:")
    if (verbose) {
      message(paste0("Storing the provided password with the name ",
                     service_name, "."))
    }
  }
}


#' Retrieve a securely-stored API key
#' @description
#' Retrieve a previously-stored API key from the user's encrypted keyring.
#' There is no argument for the keyring password and the user must provide it when interactively prompted when this function is run and the keyring is locked.
#' The keyring is managed by the user's OS and will remain unlocked for a period of time controlled by the OS, so the user will not always be prompted for a password.
#' Other trex functions for requesting data from
#' an API will use their own \code{username} and \code{api_key_name} to access keys
#' stored in this way.
#' 
#' @param username Character string. The username associated with the API key being stored, typically an email address.
#' @param api_name Character string. The abbreviated \(and case-insensitive\) name of the API, e.g. \code{"LDC"}, the password is associated with. Defaults to \code{"ldc"}.
#' @param keyring_name Optional character string. The name of the keyring that the password was stored in. If this is \code{NULL} then the keyring matching \code{generate_keyring_name(username)} will be used. Defaults to \code{NULL}.
#' @param accept_failure Logical. If \code{TRUE} then the function will return \code{NULL} if a key can't be found. Otherwise an error message will be produced. Defaults to \code{TRUE}.
#' @param verbose Logical. If \code{TRUE} the function will produce diagnostic
#'   messages. Defaults to \code{FALSE}.
get_stored_password <- function(username,
                                api_name = "ldc",
                                keyring_name = NULL,
                                accept_failure = TRUE,
                                verbose = FALSE) {
  api_name <- check_api_name(api_name)
  
  service_name <- paste0(api_name,
                         "_password")
  
  
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
    message("Getting the password stored with the name '",
            service_name, "'.")
  }
  
  output <- keyring::key_get(service = service_name,
                             username = username,
                             keyring = keyring_name) |>
    tryCatch(expr = _,
             error = function(error_message){
               !stringr::str_detect(string = error_message$message,
                                    pattern = "Element not found")
             })
  
  if (!is.character(output)) {
    if (accept_failure & !output) {
      stop(paste0("Unable to find a password for the api_name '", api_name,"'."))
      output <- NULL
    } else if (!accept_failure & !output) {
      stop(paste0("Unable to find a password for the api_name '", api_name,"'. Have you run setup_keyring() and used store_password() to save the password for your account?"))
    } else if (output) {
      stop("Some kind of error occurred with retrieving the stored password. Double check your username and api_name values.")
    }
  }
  
  output
}

store_api_token <- function(username,
                            api_name = "ldc",
                            keyring_name = NULL,
                            overwrite = FALSE,
                            verbose = FALSE) {
  api_name <- check_api_name(api_name,
                             recognized_api_names = c("ldc"))
  
  service_name <- paste0(api_name,
                         "_token")
  
  if (is.null(keyring_name)) {
    keyring_name <- get_trex_keyring_name(username = username,
                                          verbose = verbose)
  }
  
  if (verbose) {
    if (is.null(keyring_name)) {
      message("Using the system default keyring.")
    } else {
      message(paste0("Using the keyring called ",
                     keyring_name, "."))
    }
  }
  
  token_name_status <- check_token(username = username,
                                   api_name = api_name,
                                   verbose = verbose)
  
  
  if (token_name_status == "missing") {
    if (verbose) {
      message(paste0("There is no already-stored token for the username '", username, "' for the ", toupper(api_name), " API. A token will be retrieved and stored."))
    }
  } else if (token_name_status == "valid") {
    if (overwrite) {
      if (verbose) {
        message(paste0("There is a valid stored token for the username '", username, "' for the ", toupper(api_name), " API. A token will be retrieved and stored in its place."))
      }
      store_the_token <- TRUE
    } else {
      stop(paste0("There is a valid stored token for the username '", username, "' for the ", toupper(api_name), " API. To overwrite this token, set the argument overwrite to TRUE."))
    }
  } else if (token_name_status == "expired") {
    if (verbose) {
      message(paste0("There is an expired stored token for the username '", username, "' for the ", toupper(api_name), " API. A token will be retrieved and stored."))
    }
  }
  
  current_token <- get_ldc_token(username = username,
                                 keyring_name = keyring_name,
                                 password = NULL,
                                 verbose = verbose)
  
  # The token IdToken and expiration values are stored separately because the
  # keyring only supports character strings and not fancy things like lists.
  # This is fair given that it's intended for passwords.
  
  # Storing the IdToken value which will be used as the bearer credentials in
  # the header of the POST submissions
  keyring::key_set_with_value(service = paste0(api_name,
                                               "_token"),
                              keyring = keyring_name,
                              username = username,
                              password = current_token[["IdToken"]])
  # Storing the expiration time for future reference.
  keyring::key_set_with_value(service = paste0(api_name,
                                               "_token_expiration"),
                              keyring = keyring_name,
                              username = username,
                              # This'll be coerced back into a time with
                              # as.POSIXlt() when checking for expiration status
                              # but the keyring only holds character strings.
                              password = current_token[["expiration_time"]] |>
                                as.character())
  if (verbose) {
    message("Token successfully stored.")
  }
}

get_stored_token <- function(username,
                             api_name = "ldc",
                             keyring_name = NULL,
                             accept_failure = TRUE,
                             verbose = FALSE) {
  recognized_api_names <- c("ldc",
                            "rap",
                            "edit")
  api_name <- intersect(x = tolower(api_name),
                        y = recognized_api_names)
  
  if (length(api_name) != 1) {
    stop(paste0("api_name must be one of the following (case-insensitive): ",
                paste(recognized_api_names,
                      collapse = "', '"),
                "'."))
  }
  
  service_name <- paste0(api_name,
                         "_token")
  
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
    message("Looking for the API token stored with the name '",
            service_name, "'.")
  }
  
  token_status <- check_token(username = username,
                              api_name = api_name,
                              verbose = verbose)
  
  
  if (token_status == "valid") {
    output <- list("IdToken" = keyring::key_get(service = service_name,
                               username = username,
                               keyring = keyring_name),
                   "expiration_time" = keyring::key_get(service = paste0(service_name,
                                                                         "_expiration"),
                                                        username = username,
                                                        keyring = keyring_name))
  } else if (token_status == "missing") {
    if (accept_failure) {
      if (verbose) {
        message("No token was found. Returning NULL.")
      }
      output <- NULL
    } else {
      stop(paste0("No stored token was found for the username '", username,"' for the ", toupper(api_name)," API. Use store_api_token() to store a token for use."))
    }
  } else {
    if (accept_failure) {
      if (verbose) {
        message("The token found was expired. Returning NULL.")
      }
      output <- NULL
    } else {
      stop(paste0("The stored token for the username '", username,"' for the ", toupper(api_name)," API was expired. Use store_api_token() to store a token for use."))
    }
  }
  
  output
}



#' Get an access token for the Landscape Data Commons API
#' @description A function for retrieving an access token for the Landscape Data
#' Commons API based on a username and password. The token is returned as a
#' specially-formatted list that can be used as the `token` argument in
#' [fetch_ldc()], [fetch_ldc_spatial()], and [fetch_ldc_ecosite()].
#' 
#' Some data in the LDC are fully available to the public and can be accessed
#' without an account, but some are restricted to accounts with specific
#' permissions and will not be returned without a valid token. The `username`
#' and `password` values must belong to an account which has already been
#' created at https://api.landscapedatacommons.org/login/.
#' 
#' Best practice for using this function is to use it to retrieve and store a
#' token in the working environment and then provide it to the fetching
#' functions in addition to the username and password. They will use the token
#' unless it has expired in which case they will retrieve a new token
#' automatically using the provided `username` and `password`. If a fetching
#' function informs you that it retrieved a new token, continuing to fetch will
#' retrieve a new token every time because the new tokens are internal to the
#' fetching process so consider rerunning the `get_ldc_token()` to get a new
#' token to reuse before continuing to fetch. A token is valid for one hour
#' after being retrieved.
#'
#' @param username Character string. The username tied to the Landscape Data Commons API account to use.
#' @param password Character string. The password to supply to the Landscape Data Commons API.
#' @returns A list structured for use as the `token` argument in the [fetch_ldc()] family of functions.
#' @examples
#' # To get a token to use for multiple data fetchings:
#' current_token <- get_ldc_token(username = "account email address",
#'                                password = "account password")
#' # To use the token to fetch the first 100 LPI records available to the account:
#' lpi_data <- fetch_ldc(data_type = "lpi",
#'                       token = current_token,
#'                       username = "account email address",
#'                       password = "account password",
#'                       take = 100)
#' @export

get_ldc_token <- function(username,
                          keyring_name = NULL,
                          password = NULL,
                          verbose = FALSE) {
  if (is.character(username)) {
    if (length(username) > 1) {
      stop("Your username must be a single character string.")
    }
  } else {
    stop("Your username must be a single character string.")
  }
  
  if (!is.null(password)) {
    warning("It is strongly recommended that you store the password in a keyring and reference it with keyring_name rather than provide the password itself as an argument.")
  } else {
    if (is.null(keyring_name)) {
      keyring_name <- get_trex_keyring_name(username = username,
                                            verbose = verbose)
    }
    
    password <- get_stored_password(username = username,
                                    api_name = "ldc",
                                    keyring_name = keyring_name,
                                    accept_failure = FALSE,
                                    verbose = verbose)
  }
  
  if (is.character(password)) {
    if (length(password) > 1) {
      stop("Your password must be a single character string.")
    }
  } else {
    stop("Your password must be a single character string.")
  }
  
  # Attempt to get an authentication response
  authentication_response <- httr::POST(url = "https://oox5sjuicqhezohcpnbsesp32y0yrcbm.lambda-url.us-east-1.on.aws/",
                                        body = list(username = username, 
                                                    password = password),
                                        encode = "json")
  
  # What if there's an error????
  if (httr::http_error(authentication_response)) {
    stop(paste0("Retrieving authentication token from the API failed with status ",
                authentication_response$status_code))
  }
  
  output_raw_character <- rawToChar(authentication_response[["content"]])
  
  if (grepl(x = output_raw_character, pattern = "^Error")) {
    stop(output_raw_character)
  }
  
  output <- jsonlite::fromJSON(txt = rawToChar(authentication_response[["content"]]))[["AuthenticationResult"]]
  
  # We'll add an expiration time so we can check the need for a refreshed token
  # without making an API call that gets rejected.
  # This cuts 5 seconds off just as a bit of buffer.
  output[["expiration_time"]] <- Sys.time() + output[["ExpiresIn"]] - 5
  # Turns out this was overengineered, but keeping it for future reference.
  # output[["expiration_time"]] <- lubridate::as_date(lubridate::seconds(Sys.time()) + lubridate::seconds(output[["ExpiresIn"]] - 5))
  
  output
}


check_token <- function(username,
                        api_name = "ldc",
                        keyring_name = NULL,
                        verbose = FALSE) {
  recognized_api_names <- c("ldc",
                            "rap",
                            "edit")
  api_name <- intersect(x = tolower(api_name),
                        y = recognized_api_names)
  
  if (length(api_name) != 1) {
    stop(paste0("api_name must be one of the following (case-insensitive): ",
                paste(recognized_api_names,
                      collapse = "', '"),
                "'."))
  }
  
  service_name <- paste0(api_name,
                         "_token")
  
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
    message("Checking the API token stored with the name '",
            service_name, "'.")
  }
  
  current_token <- keyring::key_get(service = paste0(service_name),
                                    username = username,
                                    keyring = keyring_name) |>
    tryCatch(expr = _,
             error = function(error_message){
               NULL
             })
  
  if (is.null(current_token)) {
    if (verbose) {
      message("Unable to find a token. Returning 'missing'.")
    }
    status <- "missing"
  } else {
    if (verbose) {
      message("Found a potentially valid token. Checking expiration.")
    }
    status <- "valid"
  }
  
  
  if (status != "missing") {
    current_expiration <- keyring::key_get(service = paste0(service_name,
                                                            "_expiration"),
                                           username = username,
                                           keyring = keyring_name) |>
      tryCatch(expr = _,
               error = function(error_message){
                 NULL
               })
    
    if (is.null(current_expiration)) {
      if (verbose) {
        message("No expiration was found. Returning 'missing'.")
      }
      status <- "missing"
    } else {
      current_expiration <- as.POSIXlt(current_expiration)
      if (current_expiration < Sys.time()) {
        if (verbose) {
          message("The token appears to be expired.")
        }
        status <- "expired"
      } else {
        if (verbose) {
          message("The token appears to be valid based on the expiration.")
        }
        status <- "valid"
      }
    }
  }
  
  status
}

check_api_name <- function(api_name,
                           recognized_api_names = c("ldc",
                                                    "rap",
                                                    "edit")){
  api_name <- intersect(x = tolower(api_name),
                        y = recognized_api_names)
  
  if (length(api_name) != 1) {
    stop(paste0("api_name must be one of the following (case-insensitive): ",
                paste(recognized_api_names,
                      collapse = "', '"),
                "'."))
  }
  
  api_name
}

# check_token <- function (token, username = NULL, password = NULL, verbose = FALSE) 
# {
#   if (!is.null(token)) {
#     if (verbose) {
#       message("Checking provided token for validity.")
#     }
#     if (class(token) == "list") {
#       if (!("IdToken" %in% names(token))) {
#         stop("A valid bearer ID token must be a single character string or a single character string in a list stored at an index named 'IdToken'.")
#       }
#       else if (class(token[["IdToken"]]) != "character") {
#         stop("A valid bearer ID token must be a single character string or a single character string in a list stored at an index named 'IdToken'.")
#       }
#       if (!("expiration_time" %in% names(token))) {
#         if (verbose) {
#           message("The token doesn't have an associated expiration time and will be assumed to be valid for the next 30 minutes which may cause issues. Consider using get_ldc_token() to get a token with an accurate expiration time.")
#         }
#         token[["expiration_time"]] <- Sys.time() + 1800
#       }
#       if (verbose) {
#         message("Provided token appears to be valid.")
#       }
#     }
#     else if (class(token) == "character") {
#       if (length(token) != 1) {
#         stop("A valid bearer ID token must be a single character string or a single character string in a list stored at an index named 'IdToken'.")
#       }
#       if (verbose) {
#         message("The token doesn't have an associated expiration time and will be assumed to be valid for the next 30 minutes which may cause issues. Consider using get_ldc_token() to get a token with an accurate expiration time.")
#       }
#       token <- list(IdToken = token, expiration_time = Sys.time() + 
#                       1800)
#     }
#   }
#   else {
#     if (!identical(is.null(username), is.null(password))) {
#       if (is.null(username)) {
#         warning("No token or username provided. Returning NULL instead of getting a token.")
#       }
#       if (is.null(password)) {
#         warning("No token or password provided. Returning NULL instead of getting a token.")
#       }
#     }
#     else if (!is.null(username) & !is.null(password)) {
#       if (class(username) != "character" | length(username) > 
#           1) {
#         stop("Provided username must be a single character string.")
#       }
#       if (class(password) != "character" | length(password) > 
#           1) {
#         stop("Provided username must be a single character string.")
#       }
#       if (verbose) {
#         message("Getting a token using the provided username and password.")
#       }
#       token <- get_ldc_token(username = username, password = password)
#     }
#     else if (verbose) {
#       message("No credentials provided, returning NULL instead of getting a token.")
#     }
#   }
#   if (!is.null(token)) {
#     if (Sys.time() > token[["expiration_time"]]) {
#       if (verbose) {
#         message("Current API bearer authorization token has expired. Attempting to request a new one.")
#       }
#       if (!is.null(username) & !is.null(password)) {
#         token <- get_ldc_token(username = username, 
#                                password = password)
#       }
#       else {
#         warning("The API bearer authorization token has expired. Because username and password have not been provided, only data which do not require a token will be retrieved.")
#         token <- NULL
#       }
#     }
#   }
#   token
# }
