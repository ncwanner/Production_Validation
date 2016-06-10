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


    dataCopy = copy(data)

    ## Data quality check
    suppressMessages({
        ensureProductionInputs(dataCopy,
                               processingParameters = processingParameters,
                               formulaParameters = formulaParameters,
                               returnData = FALSE,
                               normalised = FALSE)
    })

    ## Impute only when area and yield are available and production isn't
    missingAreaHarvested =
        is.na(dataCopy[[formulaParameters$areaHarvestedValue]]) |
        dataCopy[[formulaParameters$areaHarvestedObservationFlag]] == processingParameters$missingValueObservationFlag
    nonMissingProduction =
        !is.na(dataCopy[[formulaParameters$productionValue]]) &
        dataCopy[[formulaParameters$productionObservationFlag]] != processingParameters$missingValueObservationFlag
    nonMissingYield =
        !is.na(dataCopy[[formulaParameters$yieldValue]]) &
        dataCopy[[formulaParameters$yieldObservationFlag]] != processingParameters$missingValueObservationFlag

    feasibleFilter =
        missingAreaHarvested &
        nonMissingProduction &
        nonMissingYield

    nonZeroYieldFilter =
        (dataCopy[[formulaParameters$yieldValue]] != 0)

    ## Balance area harvested
    dataCopy[feasibleFilter,
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
    dataCopy[feasibleFilter & nonZeroYieldFilter,
             `:=`(c(formulaParameters$areaHarvestedObservationFlag),
                  aggregateObservationFlag(get(formulaParameters$productionObservationFlag),
                                           get(formulaParameters$yieldObservationFlag)))]
    dataCopy[feasibleFilter & !nonZeroYieldFilter,
             `:=`(c(formulaParameters$areaHarvestedObservationFlag),
                  processingParameters$missingValueObservationFlag)]

    ## Assign method flag
    dataCopy[feasibleFilter, `:=`(c(formulaParameters$areaHarvestedMethodFlag),
                                  processingParameters$balanceMethodFlag)]
    return(dataCopy)
}
