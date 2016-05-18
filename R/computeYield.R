##' Function to compute and update yield
##'
##' @param data The data.table object containing the data.
##' @param processingParameters A list of the parameters for the production
##'   processing algorithms.  See defaultProductionParameters() for a starting
##'   point.
##' @param flagTable see data(faoswsFlagTable) in \pkg{faoswsFlag}
##' @param formulaParameter A list holding the names and parmater of formulas.
##'     See \code{productionFormulaParameters}.
##'
##' @return The updated data.table.
##'
##' @export
##'
##' @import faoswsFlag
##'

computeYield = function(data,
                        processingParameters,
                        formulaParameters,
                        flagTable = faoswsFlagTable){

    ## Data quality check
    ensureProductionInputs(data,
                           formulaParameters = formulaParameters,
                           returnData = FALSE,
                           normalised = FALSE)

    ## Balance yield values only when they're missing, and both production and
    ## area harvested are not missing
    ##
    ## NOTE (Michael): When production is zero, it would result in zero yield.
    ##                 Yield can not be zero by definition. If the production is
    ##                 zero, then yield should remain a missing value as we can
    ##                 not observe the yield.
    missingYield =
        is.na(data[[formulaParameters$yieldValue]]) |
        data[[formulaParameters$yieldObservationFlag]] == formulaParameters$missingValueObservationFlag
    nonMissingProduction =
        !is.na(data[[formulaParameters$productionValue]]) &
        data[[formulaParameters$productionObservationFlag]] != formulaParameters$missingValueObservationFlag
    nonMissingAreaHarvested =
        !is.na(data[[formulaParameters$areaHarvestedValue]]) &
        data[[formulaParameters$areaHarvestedObservationFlag]] != formulaParameters$missingValueObservationFlag
    nonZeroProduction =
        (data[[formulaParameters$productionValue]] != 0)

    feasibleFilter =
        missingYield &
        nonMissingProduction &
        nonMissingAreaHarvested &
        nonZeroProduction

    ## When area harvested (denominator) is zero, the calculation can be
    ## performed and returns NA. So a different flag should
    nonZeroAreaHarvestedFilter =
        (data[[formulaParameters$productionValue]] != 0)

    ## Calculate the yield
    data[feasibleFilter, `:=`(c(formulaParameters$yieldValue),
                              computeRatio(get(formulaParameters$productionValue),
                                           get(formulaParameters$areaHarvestedValue)) *
                              formulaParameters$unitConversion)]

    ## Assign observation flag.
    ##
    ## NOTE (Michael): If the denominator (area harvested is non-zero) then
    ##                 perform flag aggregation, if the denominator is zero,
    ##                 then assign the missing flag as the computed yield is NA.
    data[feasibleFilter & nonZeroAreaHarvestedFilter,
         `:=`(c(formulaParameters$yieldObservationFlag),
              aggregateObservationFlag(get(formulaParameters$productionObservationFlag),
                                       get(formulaParameters$areaHarvestedObservationFlag)))]
    data[feasibleFilter & !nonZeroAreaHarvestedFilter,
         `:=`(c(formulaParameters$yieldObservationFlag),
              processingParameters$missingValueObservationFlag)]

    ## Assign method flag
    data[feasibleFilter, `:=`(c(formulaParameters$yieldMethodFlag),
                              processingParameters$balanceMethodFlag)]
    return(data)
}
