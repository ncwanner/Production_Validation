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
    dataCopy = copy(data)
    ## Converting year back to database
    dataCopy[, `:=`(c(params$yearValue), as.character(.SD[[params$yearValue]]))]
    ## Restoring the 0M values
    dataWith0M =
        restore0M(dataCopy, valueVars = "Value",
                  flagVars = "flagObservationStatus")
    dataWith0M
}
