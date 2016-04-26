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
    dataCopy = copy(data)
    ## Converting year to numeric for modelling
    dataCopy[, `:=`(c(params$yearValue), as.numeric(.SD[[params$yearValue]]))]

    dataWithout0M =
        remove0M(dataCopy, valueVars = "Value", flagVars = "flagObservationStatus")
    ## HACK (Michael): Sometimes the data base return empty records
    ##                 which results in
    ##                 Value/flagObservationStatus/flagMethod to be
    ##                 missing. This temporary hack should be removed
    ##                 when the issue is resolved by the Engineering
    ##                 team.
    if(any(is.na(dataWithout0M$flagObservationStatus))){
        warning("Empty entries are omitted, this issue should be addressed at the database end")
        dataWithoutMissingFlags =
            dataWithout0M[!is.na(flagObservationStatus), ]
        return(dataWithoutMissingFlags)
    } else {
        return(dataWithout0M)
    }
}
