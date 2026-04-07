#' Landscape Data Commons table schema
#' 
#' The schema for the tables available through the LDC API as retrieved through
#' the API when the current version of the package was released. This refers to
#' the variables/columns in the LDC database as "fields".
#' 
#' @format ## `ldc_schema`
#' A data frame with 17 variables:
#' \describe{
#'   \item{rid}{The index of the record in the LDC database}
#'   \item{table_name}{The name of the table containing the field as recognized by the LDC API}
#'   \item{order}{The index of the field within its table in the LDC database}
#'   \item{field}{The name of the field in the LDC database}
#'   \item{alias}{The recognized alias of the field}
#'   \item{description}{A narrative description of the field}
#'   \item{data_type}{The data type of the values in the field}
#'   \item{length}{The maximum length of a value in the field}
#'   \item{data_type_notes}{A narrative description regarding the data_type}
#'   \item{min}{The minimum value allowed in the field}
#'   \item{max}{The maximum value allowed in the field}
#'   \item{unit}{The units of the values in the field}
#'   \item{sig_fig}{The number of significant figures for the values in the field}
#'   \item{version}{The version number}
#'   \item{uploaded}{The upload date for the schema}
#'   \item{terradactyl_alias}{The alias for the field as recognized by the R package terradactyl}
#'   \item{data_class_r}{The equivalent class in R to the data_type for the field}
#' }
#' @source <https://api.landscapedatacommons.org/docs>
#' 
"ldc_schema"