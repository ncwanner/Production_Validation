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
    missingProduction =
        is.na(dataCopy[[formulaParameters$productionValue]])
    nonMissingAreaHarvested =
        !is.na(dataCopy[[formulaParameters$areaHarvestedValue]]) &
        dataCopy[[formulaParameters$areaHarvestedObservationFlag]] != processingParameters$missingValueObservationFlag
    nonMissingYield =
        !is.na(dataCopy[[formulaParameters$yieldValue]]) &
        dataCopy[[formulaParameters$yieldObservationFlag]] != processingParameters$missingValueObservationFlag

    feasibleFilter =
        missingProduction &
        nonMissingAreaHarvested &
        nonMissingYield

    ## TODO (Michael): The yield can not be zero by definition. This should be
    ##                 removed or return an error. The input data can not
    ##                 contain zero yield.
    nonZeroYieldFilter =
        (dataCopy[[formulaParameters$yieldValue]] != 0)

    ## Calculate production
    dataCopy[feasibleFilter & nonZeroYieldFilter,
             `:=`(c(formulaParameters$productionValue),
                  get(formulaParameters$areaHarvestedValue) *
                  get(formulaParameters$yieldValue) /
                  formulaParameters$unitConversion)]
    ## Assign observation flag
    dataCopy[feasibleFilter & nonZeroYieldFilter,
             `:=`(c(formulaParameters$productionObservationFlag),
                  aggregateObservationFlag(get(formulaParameters$areaHarvestedObservationFlag),
                                           get(formulaParameters$yieldObservationFlag)))]

    ## Assign method flag
    dataCopy[feasibleFilter, `:=`(c(formulaParameters$productionMethodFlag),
                                  processingParameters$balanceMethodFlag)]
    return(dataCopy)
}
