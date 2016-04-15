##' This function performs manipulation of the data that are standard
##' after the data is retrieved from the data base.
##'
##' NOTE (Michael): The data is assumed to be normalised.
##'
##' @param data The data.table object
##' @param params The processing parameters returned by the function
##'     \code{defaultProcessingParameters}.
##'
##' @return A data.table with standard pre processing steps
##'     performed.
##'
##' @export
##'

preProcessing = function(data, params = defaultProcessingParameters()){
    ## Converting year to numeric for modelling
    data[, `:=`(c(params$yearValue), as.numeric(.SD[[params$yearValue]]))]
}
