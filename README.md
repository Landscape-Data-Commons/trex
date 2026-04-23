# trex (**T**errestrial **R**angeland data **EX**traction)

## Installation

To install trex and all documentation, use:

`devtools::install_github("landscape-data-commons/trex", build_vignettes = TRUE)`

## Background

Publicly available ecological data are important for research and land management, but accessing them can be difficult. Some data repositories like the [Landscape Data Commons (LDC)](https://landscapedatacommons.org/) and the [Ecosystem Dynamics Interpretive Tool (EDIT)](https://edit.jornada.nmsu.edu/) have user-facing APIs that can be used to retrieve large numbers of records quickly and in an automated, repeatable way. This package contains functions to assist with retrieving data from the LDC and EDIT via their APIs by constructing the relevant API calls and downloading the data for the user.

## Basic use

### Landscape Data Commons

The core LDC function is `fetch_ldc()` which retrieves data found in a specified table within the LDC. The minimum required argument is simply `data_type` which identifies the table to pull data from and—if no other arguments are given—will return *all* the data in the table. For information on which tables exist in the LDC and what variables they contain, please refer to the [LDC API documentation](https://api.landscapedatacommons.org/docs), use `fetch_ldc_metadata()`, or reference `trex::ldc_schema`

```         
# This retrieves all header information from the LDC
all_ldc_headers <- fetch_ldc(data_type = "header")
```

In most cases, you would more likely be retrieving a subset of data based on the values found in one of the variables in the table. This uses the additional argument `query_parameters` to restrict the data requested. Two of the most common variables to use are `PrimaryKey` which contains the unique identifier shared by all records associated with each sampling event and `EcologicalSiteID` which contains the ecological site ID associated with each sampling event.

```         
# This retrieves only line-point intercept data associated with the primary keys
# 20198113201104B2 and 2019885200311B1 using a list that fetch_ldc() can parse
# into a format that the API will understand
query_parameters_list <- list(PrimaryKey = list("equals" = c("20198113201104B2",
                                                             "2019885200311B1")))
lpi_data <- fetch_ldc(data_type = "lpi",
                      query_parameters = query_parameters_list)

# This retrieves only indicator data for sampling events that took place in the
# ecological site R036XB006NM
query_parameters_list <- list(EcologicalSiteID = list("equals" = c(""R036XB006NM"")))
indicator_data <- fetch_ldc(data_type = "indicators",
                            query_parameters = query_parameters_list)
```

See the documentation and vignettes for LDC functions for additional information, including how to do spatial queries with user-supplied polygons via `fetch_ldc_spatial()`.

#### Accessing data which require an account and permissions

The LDC does contain data which are not publicly available but which can be accessed with credentials that are associated with the correct permissions. You can create an LDC API account and request permissions for specific data via the [LDC API login page](https://api.landscapedatacommons.org/login/).

The trex supports the use of short-lived tokens and longer-lived API keys. The LDC API currently uses tokens, but will eventually remove token support in favor of keys.

To manage passwords, trex uses the package [keyring](https://cran.r-project.org/web/packages/keyring/index.html) which helps store credentials in a system-managed keyring. If you have API credentials that you want to use (either a password or a key) you need to have first set up a keyring using `trex::setup_keyring()`. Once a keyring is set up, keys and passwords can be stored using `store_api_key()` and `store_password()` then later retrieved with `get_stored_key()` and `get_stored_password()`. Functions like `fetch_ldc()` reference the keyring whenever they need a key or password and prompt the user to unlock it when needed, helping to prevent accidentally saving plaintext passwords.

### Ecosystem Dynamics Interpretive Tool

Comparable to `fetch_ldc()`, `fetch_edit()` queries the EDIT repository. To query EDIT, both `data_type` and `mlra` must be specified. Unlike the LDC repository, EDIT requests can only be made on a limited range of parameters, which refer to the ecological site description rather than the queried data. Queriable parameters are precipitation, frostFreeDays, elevation, slope, landform, parentMaterialOrigin, parentMaterialKind, and surfaceTexture.

```         
# This retrieves ecological site IDs and names from EDIT, from MLRA 039X
edit_ecosites <- fetch_edit(mlra = "039X", data_type = "ecosite")

# This retrives overstory community data from sites in MLRA 039X that occur at 
# slopes of 15-30%. Note: this includes all sites whose slope range overlaps with 
# the given range. For example this will return sites with slope range 25-70%.
edit_overstory_limitslope <- fetch_edit(mlra = "039X", data_type = "overstory", keys = "15:30", key_type = "slope")
```

Documentation for fetch_edit provides additional examples.
