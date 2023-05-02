## ----setup, include = FALSE---------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>"
)

## ---- eval=FALSE--------------------------------------------------------------
#  # This retrieves all header information from the LDC
#  # Here verbose = TRUE so that the function will report back on what it's doing
#  # at each step
#  all_ldc_headers <- fetch_ldc(data_type = "header",
#                               verbose = TRUE)
#  
#  # To retrieve all calculated ecological indicator values from the LDC
#  all_ldc_indicators <- fetch_ldc(data_type = "indicators")

## ---- eval=FALSE--------------------------------------------------------------
#  # To retrieve all line-point intercept records associated with the sampling
#  # event with the PrimaryKey value of 16060107170093802016-09-01
#  lpi_data_single <- fetch_ldc(keys = "16060107170093802016-09-01",
#                               key_type = "PrimaryKey",
#                               data_type = "lpi")
#  
#  # To retrieve all line-point intercept records associated with the sampling
#  # event with the PrimaryKey value of 20198113201104B2, 2019885200311B1, and
#  # 2019885200311B2
#  lpi_data_multiple <- fetch_ldc(keys = c("20198113201104B2",
#                                          "2019885200311B1",
#                                          "2019885200311B2"),
#                                 key_type = "PrimaryKey",
#                                 data_type = "lpi")

## ---- eval=FALSE--------------------------------------------------------------
#  # Retrieving canopy gap intercept data for the first 500 PrimaryKey values found
#  # in the header table
#  # Doing this in a single query will return an error, so key_chunk_size is set
#  # explicitly although this is technically unnecessary because the default is 100
#  all_ldc_headers <- fetch_ldc(data_type = "header",
#                               verbose = TRUE)
#  primarykeys_to_query <- all_ldc_headers$PrimaryKey[1:500]
#  gap_data <- fetch_ldc(keys = primarykeys_to_query,
#                        key_type = "PrimaryKey",
#                        data_type = "gap",
#                        key_chunk_size = 100)

## ---- eval=FALSE--------------------------------------------------------------
#  # Retrieving *all* line-point intercept data in the LDC would result in a 500 error
#  # from the server because there are millions of records. Setting take = 10000
#  # should solve that problem by making multiple queries instead of one.
#  # NOTE: It is absolutely not recommended that you attempt to download the entire
#  # data set this way. Please contact the data managers at the LDC if you need a full
#  # data set.
#  lpi_data <- fetch_ldc(data_type = "lpi",
#                        take = 10000,
#                        verbose = TRUE)

## ---- eval=FALSE--------------------------------------------------------------
#  # Retrieving indicator values for all sampling events that took place in
#  # the MLRA 036X (southwestern Colorado, northern New Mexico, and southeastern Utah)
#  # This will return all records where the value in EcologicalSiteID contains "036X"
#  mlra_036X_indicators <- fetch_ldc(keys = "036X",
#                                    key_type = "EcologicalSiteID",
#                                    data_type = "indicators",
#                                    exact_match = "FALSE")

## ---- eval=FALSE--------------------------------------------------------------
#  # Retrieving indicator values for all sampling events that took place in
#  # vegetation removal treatments of interest
#  # Read in the polygons as an sf object
#  treatment_polygons <- sf::st_read(dsn = "vegetation_treatments.gdb",
#                                    layer = "removal_treatment_polygons")
#  # Use the sf object to retrieve only relevant data
#  treatment_indicator_data <- fetch_ldc_spatial(polygons = treatment_polygons,
#                                                data_type = "indicators",
#                                                verbose = TRUE)
#  
#  # Retrieving line-point intercept data for all sampling events that took place
#  # in vegetation removal treatments
#  treatment_lpi_data <- fetch_ldc_spatial(polygons = treatment_polygons,
#                                          data_type = "lpi",
#                                          # The LPI table has hundreds of records
#                                          # per sampling event, so take may need
#                                          # to be specified to avoid server errors
#                                          take = 10000,
#                                          verbose = TRUE)

## ---- eval=FALSE--------------------------------------------------------------
#  # Retrieving gap values for all sampling events that took place in
#  # the ecological sites R036XB006NM and R036XB007NM
#  ecosite_gap_data <- fetch_ldc_ecosite(keys = c("R036XB006NM", "R036XB007NM"),
#                                data_type = "gap"
#                                take = 10000,
#                                verbose = TRUE)
#  
#  # Retrieving soil stability values for all sampling events that took place in
#  # the MLRA 036X (southwestern Colorado, northern New Mexico, and southeastern Utah)
#  # This will return all records where the value in EcologicalSiteID contains "036X"
#  mlra_gap_data <- fetch_ldc_ecosite(keys = "036X",
#                                     data_type = "soilstability",
#                                     exact_match = FALSE,
#                                     verbose = TRUE)

