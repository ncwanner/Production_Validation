##' Function to compute production when new area harvested and yield are given.
##'
##' @param data The data.table object containing the data.
##' @param processingParameters A list of the parameters for the
##'     production processing algorithms.  See
##'     \code{productionProcessingParameters} for a starting point.
##' @param formulaParameters A list holding the names and parmater of formulas.
##'     See \code{productionFormulaParameters}.
##'
##' @export
##'

balanceProduction = function(data,
                             processingParameters,
                             formulaParameters){


    ## Data quality check
    ensureProductionInputs(data,
                           processingParameters = processingParameters,
                           formulaParameters = formulaParameters,
                           returnData = FALSE,
                           normalised = FALSE)

    ## Impute only when area and yield are available and production isn't
    missingProduction =
        is.na(data[[formulaParameters$productionValue]]) |
        data[[formulaParameters$productionObservationFlag]] == formulaParameters$missingValueObservationFlag
    nonMissingAreaHarvested =
        !is.na(data[[formulaParameters$areaHarvestedValue]]) &
        data[[formulaParameters$areaHarvestedObservationFlag]] != formulaParameters$missingValueObservationFlag
    nonMissingYield =
        !is.na(data[[formulaParameters$yieldValue]]) &
        data[[formulaParameters$yieldObservationFlag]] != formulaParameters$missingValueObservationFlag

    feasibleFilter =
        missingProduction &
        nonMissingAreaHarvested &
        nonMissingYield

    ## TODO (Michael): The yield can not be zero by definition. This should be
    ##                 removed or return an error. The input data can not
    ##                 contain zero yield.
    nonZeroYieldFilter =
        (data[[formulaParameters$yieldValue]] != 0)

    ## Calculate production
    data[feasibleFilter & nonZeroYieldFilter,
         `:=`(c(formulaParameters$productionValue),
              get(formulaParameters$areaHarvestedValue) *
              get(formulaParameters$yieldValue) /
              formulaParameters$unitConversion)]
    ## Assign observation flag
    data[feasibleFilter & nonZeroYieldFilter,
         `:=`(c(formulaParameters$productionObservationFlag),
              aggregateObservationFlag(get(formulaParameters$areaHarvestedObservationFlag),
                                       get(formulaParameters$yieldObservationFlag)))]

    ## Assign method flag
    data[feasibleFilter, `:=`(c(formulaParameters$productionMethodFlag),
                              processingParameters$balanceMethodFlag)]
    return(data)
}
