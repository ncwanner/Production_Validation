##' Function to compute and update yield
##'
##' @param data The data.table object containing the data.
##' @param processingParameters A list of the parameters for the production
##'   processing algorithms.  See defaultProductionParameters() for a starting
##'   point.
##' @param flagTable see data(faoswsFlagTable) in \pkg{faoswsFlag}
##' @param formulaParameters A list holding the names and parmater of formulas.
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

    dataCopy = copy(data)

    ## Data quality check
    suppressMessages({
        ensureProductionInputs(dataCopy,
                               processingParameters = processingParameters,
                               formulaParameters = formulaParameters,
                               returnData = FALSE,
                               normalised = FALSE)
    })

    ## Balance yield values only when they're missing, and both production and
    ## area harvested are not missing
    ##
    ## NOTE (Michael): When production is zero, it would result in zero yield.
    ##                 Yield can not be zero by definition. If the production is
    ##                 zero, then yield should remain a missing value as we can
    ##                 not observe the yield.
    missingYield =
        is.na(dataCopy[[formulaParameters$yieldValue]]) |
        dataCopy[[formulaParameters$yieldObservationFlag]] == formulaParameters$missingValueObservationFlag
    nonMissingProduction =
        !is.na(dataCopy[[formulaParameters$productionValue]]) &
        dataCopy[[formulaParameters$productionObservationFlag]] != formulaParameters$missingValueObservationFlag
    nonMissingAreaHarvested =
        !is.na(dataCopy[[formulaParameters$areaHarvestedValue]]) &
        dataCopy[[formulaParameters$areaHarvestedObservationFlag]] != formulaParameters$missingValueObservationFlag
    nonZeroProduction =
        (dataCopy[[formulaParameters$productionValue]] != 0)

    feasibleFilter =
        missingYield &
        nonMissingProduction &
        nonMissingAreaHarvested &
        nonZeroProduction

    ## When area harvested (denominator) is zero, the calculation can be
    ## performed and returns NA. So a different flag should
    nonZeroAreaHarvestedFilter =
        (dataCopy[[formulaParameters$productionValue]] != 0)

    ## Calculate the yield
    dataCopy[feasibleFilter, `:=`(c(formulaParameters$yieldValue),
                                  computeRatio(get(formulaParameters$productionValue),
                                               get(formulaParameters$areaHarvestedValue)) *
                                  formulaParameters$unitConversion)]

    ## Assign observation flag.
    ##
    ## NOTE (Michael): If the denominator (area harvested is non-zero) then
    ##                 perform flag aggregation, if the denominator is zero,
    ##                 then assign the missing flag as the computed yield is NA.
    dataCopy[feasibleFilter & nonZeroAreaHarvestedFilter,
             `:=`(c(formulaParameters$yieldObservationFlag),
                  aggregateObservationFlag(get(formulaParameters$productionObservationFlag),
                                           get(formulaParameters$areaHarvestedObservationFlag)))]
    dataCopy[feasibleFilter & !nonZeroAreaHarvestedFilter,
             `:=`(c(formulaParameters$yieldObservationFlag),
                  processingParameters$missingValueObservationFlag)]

    ## Assign method flag
    dataCopy[feasibleFilter,
             `:=`(c(formulaParameters$yieldMethodFlag),
                  processingParameters$balanceMethodFlag)]
    return(dataCopy)
}
