.onAttach <- function(libname, pkgname) {
  # Look to see if there's plausibly a trex keyring already
  confirmed_keyring <- keyring::keyring_list()$keyring |>
    stringr::str_detect(string = _,
                        pattern = "trex_keyring") |>
    any()
  
  # If there's not a likely trex keyring, suggest to the user that they make one
  if (!confirmed_keyring) {
    paste0(rep(x = "-",
               times = getOption("width") - 2)) |>
      packageStartupMessage()
    packageStartupMessage(
      "\ntrex uses the package keyring to store and manage API keys.\n",
      "If this is the first time you have loaded trex, please configure your keyring using:\n\ntrex::setup_keyring()\n"
    )
    paste0(rep(x = "-",
               times = getOption("width") - 2)) |>
      packageStartupMessage()
  }
  # })
}