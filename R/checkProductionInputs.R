##' Check Production Inputs
##'
##' This function is designed to ensure that the provided dataset is valid.
##'
##' @param data A data.table containing the data.
##' @param processingParameters A list containing the parameters to be used in
##' the processing algorithms.  See ?defaultProcessingParameters for a starting
##' point.
##' @param returnData logical, whether the data should be returned
##'
##' @return The original data if all tests are passed
##'
##' @export
##'

checkProductionInputs = function(data, processingParameters, returnData = TRUE){

    ### Basic checks
    stopifnot(is(data, "data.table"))
    stopifnot(is(processingParameters, "list"))

    ### Check for parameters
    param = processingParameters
    stopifnot(is.character(c(param$productionValue,
                             param$productionObservationFlag,
                             param$productionMethodFlag,
                             param$yieldValue,
                             param$yieldObservationFlag,
                             param$yieldMethodFlag,
                             param$areaHarvestedValue,
                             param$areaHarvestedObservationFlag,
                             param$areaHarvestedMethodFlag,
                             param$yearValue,
                             param$areaVar)))
    stopifnot(is.logical(c(param$removePriorImputation,
                           param$removeConflictValues)))
    stopifnot(is.character(c(param$imputationObservationFlag,
                             param$missingValueObservationFlag,
                             param$imputationMethodFlag)))

    ### Make sure all column name variables exist in data
    columnNames = c(processingParameters$productionValue,
                    processingParameters$productionObservationFlag,
                    processingParameters$productionMethodFlag,
                    processingParameters$yieldValue,
                    processingParameters$yieldObservationFlag,
                    processingParameters$yieldMethodFlag,
                    processingParameters$areaHarvestedValue,
                    processingParameters$areaHarvestedObservationFlag,
                    processingParameters$areaHarvestedMethodFlag,
                    processingParameters$yearValue,
                    processingParameters$areaVar)
    missingColumns = ! columnNames %in% colnames(data)
    if(any(missingColumns))
        stop("The following columns do not exist in data but should (or the",
             "parameters in the global environment should be corrected):\n\t",
             paste(columnNames[missingColumns], collapse="\n\t"))

    ## Conflict values
    productionAreaHarvestedNotMissing =
        !is.na(data[[processingParameters$productionValue]]) &
        !is.na(data[[processingParameters$areaHarvestedValue]])
    productionZeroAreaNonZero =
        data[[processingParameters$productionValue]] == 0 &
        data[[processingParameters$areaHarvestedValue]] != 0
    productionNonZeroAreaHarvestedZero =
        data[[processingParameters$productionValue]] != 0 &
        data[[processingParameters$areaHarvestedValue]] == 0

    conflictProductionAreaHarvested =
        which(productionAreaHarvestedNotMissing &
              productionZeroAreaNonZero &
              productionNonZeroAreaHarvestedZero)
    if(length(conflictProductionAreaHarvested) > 0)
        stop("Conflict value exist in production area harvested")


    ## Zero yield
    if(any(data[[processingParameters$yieldValue]] == 0))
        stop("Yield can not be zero by definition")

    if(returnData)
        return(data)
}
