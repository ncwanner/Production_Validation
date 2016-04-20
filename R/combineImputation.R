combineImputation = function(imputationWithEstimates,
                             imputationWithoutEstimates){
    combinedImputation = rbind(imputationWithoutEstimates,
                               imputationWithEstimates)
    combinedImputation[, useEstimates := NULL]
    combinedImputation                
}
