##' This is a wrapper for all the data manipulation steps before the preparation
##' of the imputation.
##'
##' @param data The data
##' @param processingParameters A list of the parameters for the production
##'   processing algorithms.  See defaultProductionParameters() for a starting
##'   point.
##'
##' @export
##'
##' @return Currently it returns the passed data.table after performing some
##'   checks and clean up of the data.  Eventually, it should modify the
##'   data.table in place, but this will require an update to data.table (see
##'   comment by the return statement).
##'

processProductionDomain = function(data, processingParameters){
    dataCopy = copy(data)

    ## Data quality check
    checkProductionImputs(dataCopy,
                          processingParameters = processingParameters,
                          returnData = FALSE)

    ## processingParameters will be referenced alot, so rename to param
    param = processingParameters
    ## Remove prior imputations
    if(param$removePriorImputation){
        ## Remove current imputation (flag = I, e)
        removeImputationEstimation(data = dataCopy,
                                   value = param$areaHarvestedValue,
                                   observationFlag =
                                       param$areaHarvestedObservationFlag,
                                   methodFlag = param$areaHarvestedMethodFlag,
                                   missingObservationFlag =
                                       param$missingValueObservationFlag,
                                   imputationEstimationObservationFlag =
                                       param$imputationObservationFlag,
                                   imputationEstimationMethodFlag =
                                       param$imputationMethodFlag)
        removeImputationEstimation(data = dataCopy,
                                   value = param$yieldValue,
                                   observationFlag = param$yieldObservationFlag,
                                   methodFlag = param$yieldMethodFlag,
                                   missingObservationFlag =
                                       param$missingValueObservationFlag,
                                   imputationEstimationObservationFlag =
                                       param$imputationObservationFlag,
                                   imputationEstimationMethodFlag =
                                       param$imputationMethodFlag)
        removeImputationEstimation(data = dataCopy,
                                   value = param$productionValue,
                                   observationFlag =
                                       param$productionObservationFlag,
                                   methodFlag = param$productionMethodFlag,
                                   missingObservationFlag =
                                       param$missingValueObservationFlag,
                                   imputationEstimationObservationFlag =
                                       param$imputationObservationFlag,
                                   imputationEstimationMethodFlag =
                                       param$imputationMethodFlag)

        ## Remove historical imputation (flag = E, e)
        ##
        ## NOTE (Michael): This is however, incorrectly mapped in the database.
        ##                 The old imputation should also have the flag
        ##                 combination (I, e). When this is corrected, the
        ##                 following chunk can be removed.

        removeImputationEstimation(data = dataCopy,
                                   value = param$areaHarvestedValue,
                                   observationFlag =
                                       param$areaHarvestedObservationFlag,
                                   methodFlag = param$areaHarvestedMethodFlag,
                                   missingObservationFlag =
                                       param$missingValueObservationFlag,
                                   imputationEstimationObservationFlag =
                                       param$manualEstimationObservationFlag,
                                   imputationEstimationMethodFlag =
                                       param$imputationMethodFlag)
        removeImputationEstimation(data = dataCopy,
                                   value = param$yieldValue,
                                   observationFlag = param$yieldObservationFlag,
                                   methodFlag = param$yieldMethodFlag,
                                   missingObservationFlag =
                                       param$missingValueObservationFlag,
                                   imputationEstimationObservationFlag =
                                       param$manualEstimationObservationFlag,
                                   imputationEstimationMethodFlag =
                                       param$imputationMethodFlag)
        removeImputationEstimation(data = dataCopy,
                                   value = param$productionValue,
                                   observationFlag =
                                       param$productionObservationFlag,
                                   methodFlag = param$productionMethodFlag,
                                   missingObservationFlag =
                                       param$missingValueObservationFlag,
                                   imputationEstimationObservationFlag =
                                       param$manualEstimationObservationFlag,
                                   imputationEstimationMethodFlag =
                                       param$imputationMethodFlag)
    }

    emptyEntry = (is.na(dataCopy[[param$productionObservationFlag]]) |
                  is.na(dataCopy[[param$yieldObservationFlag]])|
                  is.na(dataCopy[[param$areaHarvestedObservationFlag]]))

    dataCopy = dataCopy[!emptyEntry, ]

    if(param$removeManualEstimation){
        ## Removing manual estimation (flag = E, f)
        removeImputationEstimation(data = dataCopy,
                                   value = param$areaHarvestedValue,
                                   observationFlag =
                                       param$areaHarvestedObservationFlag,
                                   methodFlag = param$areaHarvestedMethodFlag,
                                   missingObservationFlag =
                                       param$missingValueObservationFlag,
                                   imputationEstimationObservationFlag =
                                       param$manualEstimationObservationFlag,
                                   imputationEstimationMethodFlag =
                                       param$manualEstimationMethodFlag)
        removeImputationEstimation(data = dataCopy,
                                   value = param$yieldValue,
                                   observationFlag = param$yieldObservationFlag,
                                   methodFlag = param$yieldMethodFlag,
                                   missingObservationFlag =
                                       param$missingValueObservationFlag,
                                   imputationEstimationObservationFlag =
                                       param$manualEstimationObservationFlag,
                                   imputationEstimationMethodFlag =
                                       param$manualEstimationMethodFlag)
        removeImputationEstimation(data = dataCopy,
                                   value = param$productionValue,
                                   observationFlag =
                                       param$productionObservationFlag,
                                   methodFlag = param$productionMethodFlag,
                                   missingObservationFlag =
                                       param$missingValueObservationFlag,
                                   imputationEstimationObservationFlag =
                                       param$manualEstimationObservationFlag,
                                   imputationEstimationMethodFlag =
                                       param$manualEstimationMethodFlag)
    }

    ## Remove yield that are zero
    dataCopy = removeZeroYield(data = dataCopy,
                        yieldValue = param$yieldValue,
                        yieldObsFlag = param$yieldObservationFlag,
                        yieldMethodFlag = param$yieldMethodFlag)

    return(dataCopy)
}
