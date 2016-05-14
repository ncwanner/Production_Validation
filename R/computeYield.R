##' Function to compute and update yield
##'
##' @param data The data.table object containing the data.
##' @param processingParameters A list of the parameters for the production
##'   processing algorithms.  See defaultProductionParameters() for a starting
##'   point.
##' @param flagTable see data(faoswsFlagTable) in \pkg{faoswsFlag}
##'
##' @return The updated data.table.
##'
##' @export
##'
##' @import faoswsFlag
##'

computeYield = function(data,
                        processingParameters,
                        flagTable = faoswsFlagTable){

    if(!exists("ensuredProductionData") || !ensuredProductionData)
        ensureProductionInputs(data = data,
                               processingParameters = processingParameters)
    stopifnot(faoswsUtil::checkMethodFlag(processingParameters$balanceMethodFlag))

    ## Abbreviate processingParameters since it is used alot
    param = processingParameters

    ## Balance yield values only when they're missing, and both production and
    ## area harvested are not missing
    ##
    ## NOTE (Michael): When production is zero, it would result in zero yield.
    ##                 Yield can not be zero by definition. If the production is
    ##                 zero, then yield should remain a missing value as we can
    ##                 not observe the yield.
    missingYield =
        is.na(data[[param$yieldValue]]) |
        data[[param$yieldObservationFlag]] == param$missingValueObservationFlag
    nonMissingProduction =
        !is.na(data[[param$productionValue]]) &
        data[[param$productionObservationFlag]] != param$missingValueObservationFlag
    nonMissingAreaHarvested =
        !is.na(data[[param$areaHarvestedValue]]) &
        data[[param$areaHarvestedObservationFlag]] != param$missingValueObservationFlag
    nonZeroProduction =
        (data[[param$productionValue]] != 0)

    feasibleFilter =
        missingYield &
        nonMissingProduction &
        nonMissingAreaHarvested &
        nonZeroProduction

    ## When area harvested (denominator) is zero, the calculation can be
    ## performed and returns NA. So a different flag should
    nonZeroAreaHarvestedFilter =
        (data[[param$productionValue]] != 0)

    ## Calculate the yield
    data[feasibleFilter, `:=`(c(param$yieldValue),
                              computeRatio(get(param$productionValue),
                                           get(param$areaHarvestedValue)) *
                              param$unitConversion)]

    ## Assign observation flag.
    ##
    ## NOTE (Michael): If the denominator (area harvested is non-zero) then
    ##                 perform flag aggregation, if the denominator is zero,
    ##                 then assign the missing flag as the computed yield is NA.
    data[feasibleFilter & nonZeroAreaHarvestedFilter,
         `:=`(c(param$yieldObservationFlag),
              aggregateObservationFlag(get(param$productionObservationFlag),
                                       get(param$areaHarvestedObservationFlag)))]
    data[feasibleFilter & !nonZeroAreaHarvestedFilter,
         `:=`(c(param$yieldObservationFlag), param$missingValueObservationFlag)]

    ## Assign method flag
    data[feasibleFilter, `:=`(c(param$yieldMethodFlag),
                              param$balanceMethodFlag)]
    return(data)
}
