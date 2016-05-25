##' Function to compute area harvested when new production and yield are given.
##'
##' @param data The data.table object containing the data.
##' @param processingParameters A list of the parameters for the production
##'     processing algorithms. See \code{productionProcessingParameters} for a
##'     starting point.
##' @param formulaParameters A list holding the names and parmater of formulas.
##'     See \code{productionFormulaParameters}.
##'
##' @export
##'

balanceAreaHarvested = function(data,
                                processingParameters,
                                formulaParameters){


    ## Data quality check
    ensureProductionInputs(data,
                           processingParameters = processingParameters,
                           formulaParameters = formulaParameters,
                           returnData = FALSE,
                           normalised = FALSE)


    ## Impute only when area and yield are available and production isn't
    missingAreaHarvested =
        is.na(data[[formulaParameters$areaHarvestedValue]]) |
        data[[formulaParameters$areaHarvestedObservationFlag]] == formulaParameters$missingValueObservationFlag
    nonMissingProduction =
        !is.na(data[[formulaParameters$productionValue]]) &
        data[[formulaParameters$productionObservationFlag]] != formulaParameters$missingValueObservationFlag
    nonMissingYield =
        !is.na(data[[formulaParameters$yieldValue]]) &
        data[[formulaParameters$yieldObservationFlag]] != formulaParameters$missingValueObservationFlag

    feasibleFilter =
        missingAreaHarvested &
        nonMissingProduction &
        nonMissingYield

    nonZeroYieldFilter =
        (data[[formulaParameters$yieldValue]] != 0)

    ## Balance area harvested
    data[feasibleFilter,
         `:=`(c(formulaParameters$areaHarvestedValue),
              computeRatio(get(formulaParameters$productionValue),
                           get(formulaParameters$yieldValue)) *
              formulaParameters$unitConversion)]
    ## Assign observation flag.
    ##
    ## NOTE (Michael): If the denominator (yield is non-zero) then
    ##                 perform flag aggregation, if the denominator is zero,
    ##                 then assign the missing flag as the computed yield is NA.
    ##
    ## NOTE (Michael): Although the yield should never be zero by definition.
    data[feasibleFilter & nonZeroYieldFilter,
         `:=`(c(formulaParameters$areaHarvestedObservationFlag),
              aggregateObservationFlag(get(formulaParameters$productionObservationFlag),
                                       get(formulaParameters$yieldObservationFlag)))]
    data[feasibleFilter & !nonZeroYieldFilter,
         `:=`(c(formulaParameters$areaHarvestedObservationFlag),
              processingParameters$missingValueObservationFlag)]

    ## Assign method flag
    data[feasibleFilter, `:=`(c(formulaParameters$areaHarvestedMethodFlag),
                              processingParameters$balanceMethodFlag)]
    return(data)
}
