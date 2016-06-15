##' This function creates the name for the imputation object to be created and
##' loaded. This is to ensure the names are consistent between the two moduels.
##'
##' @param item The item code for the imputation (e.g. 0111 for wheat)
##'
##' @return A character name
##'
##' @export
createImputationObjectName = function(item){
    paste0("imputation_", as.character(item), ".rds")
}
