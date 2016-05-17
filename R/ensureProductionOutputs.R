ensureProductionOutputs = function(data,
                                   processingParameters,
                                   domain = "agriculture",
                                   dataset = "aproduction",
                                   returnData = TRUE,
                                   normalised = TRUE){
    if(normalised){
        dataCopy = denormalise(dataCopy, "measuredElement")
    }

    with(processingParameters,
         ## Check data inputs
         ensureDataInput(data = dataCopy,
                         requiredColumn = c(productionValue,
                                            productionObservationFlag,
                                            productionMethodFlag,
                                            yieldValue,
                                            yieldObservationFlag,
                                            yieldMethodFlag,
                                            areaHarvestedValue,
                                            areaHarvestedObservationFlag,
                                            areaHarvestedMethodFlag,
                                            yearValue,
                                            areaVar),
                         returnData = FALSE)


         ## Ensure there is no production is zero while area harvested is non
         ## zero, vice versa.
         ensureNoConflictingZero(data = dataCopy,
                                 valueColumn1 = productionValue,
                                 valueColumn2 = areaHarvestedValue,
                                 returnData = FALSE,
                                 normalised = FALSE)

         ## Ensure yield contains no zero
         ensureNoZeroValue(data = dataCopy,
                           noZeroValueColumn = yieldValue,
                           returnData = FALSE)

         ## Ensure flags are valid
         ensureFlagValidity(data = dataCopy,
                            normalised = FALSE)

         ## Ensure production is balanced
         ##
         ## NOTE (Michael): This may be optional in input, but mandatory in
         ##                 output
         ensureProductionBalanced(dataToBeSaved = dataCopy,
                                  areaVar = areaHarvestedValue,
                                  yieldVar = yieldValue,
                                  prodVar = productionValue,
                                  conversion,
                                  returnData = FALSE,
                                  normalised = FALSE)

         ## Ensure time series are imputed
         ensureTimeSeriesImputed(dataToBeSaved = dataCopy,
                                 key = c(areaVar, itemVar, elementVar),
                                 returnData = FALSE,
                                 normalised = FALSE)

         ## Ensure protected data are not over-written
         ensureProtectedData(dataToBeSaved = dataCopy,
                             areaVar = areaVar,
                             itemVar = itemVar,
                             elementVar = elementVar
                             yearVar = yearVar,
                             flagObservationVar = flagObsservationVar,
                             flagMethodVar = flagMethodVar,
                             returnData = FALSE,
                             normalised = FALSE)

         ## Ensure the identity is calculated.
         ensureIdentityCalculated(dataToBeSaved = dataCopy,
                                  areaVar = areaHarvestedValue,
                                  yieldVar = yieldValue,
                                  prodVar = productionValue,
                                  returnData = FALSE,
                                  normalised = FALSE)
         )

    if(normalised){
        dataCopy = normalise(dataCopy)
    }

    if(returnData)
        return(dataCopy)
}

