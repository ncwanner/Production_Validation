##' Ensure Production Inputs
##'
##' This function is designed to ensure that the provided dataset is valid.  In
##' particular, it coerces column types: all values are coerced to numeric (
##' instead of integer, which can cause problems) and all flags are coerced to
##' character (instead of logical, which occurs if the flag is set to NA).
##' Also, it ensures data is a data.table.
##'
##' @param data A data.table containing the data.
##' @param processingParameters A list containing the parameters to be used in
##' the processing algorithms.  See ?defaultProcessingParameters for a starting
##' point.
##'
##' @export
##'

ensureProductionInputs = function(data, processingParameters){

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


    ## TODO (Michael): Adding in additional tests

    ## Conflict values

    ## Zero yield

    ## Inconsistent flags


}
