##' This function performs manipulation of the data that are standard
##' before the data is saved back to the data base.
##'
##' NOTE (Michael): The data is assumed to be normalised.
##'
##' @param data The data.table object
##' @param params The processing parameters returned by the function
##'     \code{defaultProcessingParameters}.
##'
##' @return A data.table with standard post processing steps
##'     performed.
##'
##' @export
##'

postProcessing = function(data, params = defaultProcessingParameters()){
    ## Converting year back to database
    data[, `:=`(c(params$yearValue), as.character(.SD[[params$yearValue]]))]
}

