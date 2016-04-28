selectUseEstimate = function(data, useEstimatesTable, useEstimates){
    dataCopy = copy(data)
    if(useEstimates){
        imputationWithEstimates =
            merge(dataCopy,
                  useEstimatesTable[(useEstimates), ],
                  by = c("geographicAreaM49", "measuredElement"))
        return(imputationWithEstimates)
    } else {
        ## Filter to only include series with enough data:
        imputationWihoutEstimates =
            merge(dataCopy,
                  useEstimatesTable[!(useEstimates), ],
                  by = c("geographicAreaM49", "measuredElement"))
        return(imputationWihoutEstimates)
    }
}
