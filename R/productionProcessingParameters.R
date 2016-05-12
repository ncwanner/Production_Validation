productionProcessingParameters = function(datasetConfig,
                                          productionCode = "5510",
                                          areaHarvestedCode = "5312",
                                          yieldCode = "5416",
                                          removePriorImputation = TRUE,
                                          removeManualEstimates = TRUE,
                                          removeConflictValues = TRUE,
                                          imputationObservationFlag = "I",
                                          imputationMethodFlag = "e",
                                          balanceMethodFlag = "i",
                                          manualEstimateMethodFlag = "f",
                                          missingValueObservationFlag = "M"
                                          missingValueMethodFlag = "u",
                                          protectedMethodFlag = c("-", "q", "p", "h", "c")){
    ## HACK (Michael): There is no information on how to configure this, and
    ##                 thus it is hard coded.
    areaVar = datasetConfig$dimensions[1]
    itemVar = datasetConfig$dimensions[3]
    elementVar = datasetConfig$dimensions[2]
    yearVar = datasetConfig$timeDimension
    flagObservationVar = datasetConfig$flags[1]
    flagMethodVar = datasetConfig$flags[2]
    valueVar = "Value"

    ## Return the list of parameters
    list(productionValue =
             paste0(c(valueVar, elementVar, productionCode), collapse = "_"),
         areaHarvestedValue =
             paste0(c(valueVar, elementVar, areaHarvestedCode), collapse = "_"),
         yieldValue =
             paste0(c(valueVar, elementVar, yieldCode), collapse = "_"),
         productionObservationFlag =
             paste0(c(flagObservationVar, elementVar, productionCode),
                    collapse = "_"),
         areaHarvestedObservationFlag =
             paste0(c(flagObservationVar, elementVar, areaHarvestedCode),
                    collapse = "_"),
         yieldObservationFlag =
             paste0(c(flagObservationVar, elementVar, yieldCode), collapse = "_"),
         productionMethodFlag =
             paste0(c(flagMethodVar, elementVar, productionCode), collapse = "_"),
         areaHarvestedMethodFlag =
             paste0(c(flagMethodVar, elementVar, areaHarvestedCode),
                    collapse = "_"),
         yieldMethodFlag =
             paste0(c(flagMethodVar, elementVar, yieldCode), collapse = "_"),
         areaVar = areaVar,
         yearVar = yearVar,
         itemVar = itemVar,
         elementVar = elementVar,
         flagObservationVar = flagObservationVar,
         flagMethodVar = flagMethodVar,
         valueVar = valueVar,
         removePriorImputation = removePriorImputation,
         removeManualEstimates = removeManualEstimates,
         removeConflictValues = removeConflictValues,
         imputationObservationFlag = imputationObservationFlag,
         imputationMethodFlag = imputationMethodFlag,
         balanceMethodFlag = balanceMethodFlag,
         manualEstimateMethodFlag = manualEstimateMethodFlag,
         missingValueObservationFlag = missingValueObservationFlag,
         missingValueMethodFlag = missingValueMethodFlag,
         protectedMethodFlag = protectedMethodFlag
         )

}
