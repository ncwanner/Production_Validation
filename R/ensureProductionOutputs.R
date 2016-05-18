##' Check Production Inputs
##'
##' This function is designed to ensure that the dataset to be saved is valid.
##'
##' @param data A data.table containing the data.
##' @param processingParameters A list containing the parameters to be used in
##'     the processing algorithms. See ?defaultProcessingParameters for a
##'     starting point.
##' @param testImputed logical, whether to test imputation result
##' @param testCalculated logical, whether to test calculated result.
##' @param domain The domain to be saved back
##' @param dataset The dataset to be saved back
##' @param returnData logical, whether the data should be returned
##' @param normalised logical, whether the data is normalised
##'
##' @return The original data if all tests are passed
##'
##' @export
##'

ensureProductionOutputs = function(data,
                                   processingParameters,
                                   testImputed = TRUE,
                                   testCalculated = TRUE,
                                   domain = "agriculture",
                                   dataset = "aproduction",
                                   returnData = TRUE,
                                   normalised = TRUE){
    if(normalised){
        dataCopy = denormalise(dataCopy, "measuredElement")
    }

    with(processingParameters,
    {

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
                                           yearVar,
                                           areaVar),
                        returnData = FALSE)


        ## Ensure there is no production is zero while area harvested is non
        ## zero, vice versa.
        ensureNoConflictingZero(data = dataCopy,
                                valueColumn1 = productionValue,
                                valueColumn2 = areaHarvestedValue,
                                returnData = FALSE,
                                normalised = FALSE)

        ## Ensure the range of values are correct
        ##
        ## NOTE (Michael): Yield can not be equal to zero
        ensureValueRange(data = dataCopy,
                         ensureColumn = yieldValue,
                         min = 0,
                         max = Inf,
                         includeEndPoint = FALSE)
        ensureValueRange(data = dataCopy,
                         ensureColumn = areaHarvestedValue,
                         min = 0,
                         max = Inf)
        ensureValueRange(data = dataCopy,
                         ensureColumn = productionValue,
                         min = 0,
                         max = Inf)

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

        if(testImputed){
            ## Ensure time series are imputed
            ensureTimeSeriesImputed(dataToBeSaved = dataCopy,
                                    key = c(areaVar, itemVar, elementVar),
                                    returnData = FALSE,
                                    normalised = FALSE)
        }

        if(testCalculated){
            ## Ensure the identity is calculated.
            ensureIdentityCalculated(dataToBeSaved = dataCopy,
                                     areaVar = areaHarvestedValue,
                                     yieldVar = yieldValue,
                                     prodVar = productionValue,
                                     returnData = FALSE,
                                     normalised = FALSE)
        }

        ## Ensure protected data are not over-written
        ensureProtectedData(dataToBeSaved = dataCopy,
                            domain = domain,
                            dataset = dataset,
                            areaVar = areaVar,
                            itemVar = itemVar,
                            elementVar = elementVar,
                            yearVar = yearVar,
                            flagObservationVar = flagObsservationVar,
                            flagMethodVar = flagMethodVar,
                            returnData = FALSE,
                            normalised = FALSE)

    })

    if(normalised){
        dataCopy = normalise(dataCopy)
    }

    if(returnData)
        return(dataCopy)
}
