getImputedValues = function(data, useEstimatesTable, useEstimates){
    dataCopy = copy(data)
    if(useEstimates){
        imputedData = dataCopy[!flagMethod == "i", ]
        imputationWithEstimates =
            merge(imputedData,
                  useEstimatesTable[(useEstimates), ],
                  by = c("geographicAreaM49", "measuredElement"))
        return(imputationWithEstimates)
    } else {
        imputedData = dataCopy[!flagMethod == "i", ]
        ## Filter to only include series with enough data:
        imputationWihoutEstimates =
            merge(imputedData,
                  useEstimatesTable[!(useEstimates), ],
                  by = c("geographicAreaM49", "measuredElement"))
        return(imputationWihoutEstimates)
    }
}
