##' Function to compute production when new area harvested and yield are given.
##'
##' @param data The data.table object containing the data.
##' @param processingParameters A list of the parameters for the
##'     production processing algorithms.  See
##'     \code{productionProcessingParameters} for a starting point.
##'     1.
##'
##' @export
##'

balanceProduction = function(data,
                             processingParameters){

    ## Data Quality Checks
    if(!exists("ensuredProductionData") || !ensuredProductionData)
        ensureProductionInputs(data = data,
                               processingParameters = processingParameters)

    ## Type "param" instead of "processingParameters"
    param = processingParameters

    ## Impute only when area and yield are available and production isn't
    missingProduction =
        is.na(data[[param$productionValue]]) |
        data[[param$productionObservationFlag]] == param$missingValueObservationFlag
    nonMissingAreaHarvested =
        !is.na(data[[param$areaHarvestedValue]]) &
        data[[param$areaHarvestedObservationFlag]] != param$missingValueObservationFlag
    nonMissingYield =
        !is.na(data[[param$yieldValue]]) &
        data[[param$yieldObservationFlag]] != param$missingValueObservationFlag

    feasibleFilter =
        missingProduction &
        nonMissingAreaHarvested &
        nonMissingYield

    ## TODO (Michael): The yield can not be zero by definition. This should be
    ##                 removed or return an error. The input data can not
    ##                 contain zero yield.
    nonZeroYieldFilter =
        (data[[param$yieldValue]] != 0)

    ## Calculate production
    data[feasibleFilter & nonZeroYieldFilter,
         `:=`(c(param$productionValue),
              sapply(get(param$areaHarvestedValue) *
                     get(param$yieldValue) /
                     param$unitConversion, FUN = roundResults))]
    ## Assign observation flag
    data[feasibleFilter & nonZeroYieldFilter,
         `:=`(c(param$productionObservationFlag),
              aggregateObservationFlag(get(param$areaHarvestedObservationFlag),
                                       get(param$yieldObservationFlag)))]

    ## Assign method flag
    data[feasibleFilter, `:=`(c(param$productionMethodFlag),
                              param$balanceMethodFlag)]
    return(data)
}
