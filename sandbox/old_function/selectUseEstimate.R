##' Subset imputed data to whether estimates should be used.
##'
##' @param data The imputed dataset
##' @param useEstimatesTable The table returned by the function
##'     \code{useEstimateForTimeSeriesImputation}
##' @param useEstimates Whether the estimates should be used.
##'
##' @return The subset of the imputed data
##' @export
##'

selectUseEstimate = function(data, useEstimatesTable, useEstimates){
    dataCopy = copy(data)
    if(useEstimates){
        imputationWithEstimates =
            merge(dataCopy,
                  useEstimatesTable[(useEstimates), ],
                  by = c("geographicAreaM49"),
                  allow.cartesian = TRUE)
        return(imputationWithEstimates)
    } else {
        ## Filter to only include series with enough data:
        imputationWihoutEstimates =
            merge(dataCopy,
                  useEstimatesTable[!(useEstimates), ],
                  by = c("geographicAreaM49"),
                  allow.cartesian = TRUE)
        return(imputationWihoutEstimates)
    }
}
