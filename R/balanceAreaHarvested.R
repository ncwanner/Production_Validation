##' Function to compute area harvested when new production and yield are given.
##'
##' @param data The data.table object containing the data.
##' @param processingParameters A list of the parameters for the production
##'     processing algorithms. See \code{productionProcessingParameters} for a
##'     starting point.
##'
##' @export
##'

balanceAreaHarvested = function(data,
                                processingParameters){

    ## Data Quality Checks
    if(!exists("ensuredProductionData") || !ensuredProductionData)
        ensureProductionInputs(data = data,
                               processingParameters = processingParameters)

    ## Save clutter by renaming "processingParameters" to "p" locally.
    param = processingParameters

    ## Impute only when area and yield are available and production isn't
    missingAreaHarvested =
        is.na(data[[param$areaHarvestedValue]]) |
        data[[param$areaHarvestedObservationFlag]] == param$missingValueObservationFlag
    nonMissingProduction =
        !is.na(data[[param$productionValue]]) &
        data[[param$productionObservationFlag]] != param$missingValueObservationFlag
    nonMissingYield =
        !is.na(data[[param$yieldValue]]) &
        data[[param$yieldObservationFlag]] != param$missingValueObservationFlag

    feasibleFilter =
        missingAreaHarvested &
        nonMissingProduction &
        nonMissingYield

    nonZeroYieldFilter =
        (data[[param$yieldValue]] != 0)

    ## Balance area harvested
    data[feasibleFilter, `:=`(c(param$areaHarvestedValue),
                              sapply(computeRatio(get(param$productionValue),
                                                  get(param$yieldValue)) *
                                     param$unitConversion, roundResults))]
    ## Assign observation flag.
    ##
    ## NOTE (Michael): If the denominator (yield is non-zero) then
    ##                 perform flag aggregation, if the denominator is zero,
    ##                 then assign the missing flag as the computed yield is NA.
    ##
    ## NOTE (Michael): Although the yield should never be zero by definition.
    data[feasibleFilter & nonZeroYieldFilter,
         `:=`(c(param$areaHarvestedObservationFlag),
              aggregateObservationFlag(get(param$productionObservationFlag),
                                       get(param$yieldObservationFlag)))]
    data[feasibleFilter & !nonZeroYieldFilter,
         `:=`(c(param$areaHarvestedObservationFlag),
              param$missingValueObservationFlag)]

    ## Assign method flag
    data[feasibleFilter, `:=`(c(param$areaHarvestedMethodFlag),
                              param$balanceMethodFlag)]
    return(data)
}
