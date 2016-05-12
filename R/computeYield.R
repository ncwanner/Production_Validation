##' Function to compute and update yield
##'
##' @param data The data.table object containing the data.
##' @param processingParameters A list of the parameters for the production
##'   processing algorithms.  See defaultProductionParameters() for a starting
##'   point.
##' @param newMethodFlag The flag to be used to update the yield method flag
##'   when imputation occurs.
##' @param flagTable see data(faoswsFlagTable) in \pkg{faoswsFlag}
##' @param unitConversion Yield is computed as (production) / (area) and
##'   multiplied by unitConversion.  This parameter defaults to 1.
##' @param normalized Is the input data normalised.
##'
##' @return The updated data.table.  This is important in the case where the
##'   data is normalized, as the data.table must be cast and reshaped (and thus
##'   can't be modified by reference).
##'
##' @export
##'
##' @import faoswsFlag
##'

computeYield = function(data, processingParameters, normalized = FALSE,
                        newMethodFlag = "i",
                        flagTable = faoswsFlagTable, unitConversion = 1){

    if(!exists("ensuredProductionData") || !ensuredProductionData)
        ensureProductionInputs(data = data,
                               processingParameters = processingParameters)
    stopifnot(faoswsUtil::checkMethodFlag(newMethodFlag))

    ## Abbreviate processingParameters since it is used alot
    param = processingParameters

    ## Balance yield values only when they're missing
    missingYield = is.na(data[[param$yieldValue]]) |
        data[[param$yieldObservationFlag]] == param$missingValueObservationFlag
    filter = missingYield &
        !is.na(data[[param$productionValue]]) &
        !is.na(data[[param$areaHarvestedValue]]) &
        data[[param$productionObservationFlag]] != param$missingValueObservationFlag &
        data[[param$areaHarvestedObservationFlag]] != param$missingValueObservationFlag

    data[filter, `:=`(c(param$yieldValue),
                      computeRatio(get(param$productionValue),
                                   get(param$areaHarvestedValue)) *
                      unitConversion)]
    data[filter,
         `:=`(c(param$yieldObservationFlag),
              aggregateObservationFlag(get(param$productionObservationFlag),
                                       get(param$areaHarvestedObservationFlag)))]
    data[filter, c(param$yieldMethodFlag) := newMethodFlag]
    ## If yieldValue is still NA, make sure observation flag is "M".  Note:
    ## this can happen by taking 0 production / 0 area.
    data[is.na(get(param$yieldValue)),
         `:=`(c(param$yieldObservationFlag), param$missingValueObservationFlag)]
         data[is.na(get(param$yieldValue)),
              `:=`(c(param$yieldMethodFlag), param$missingValueMethodFlag)]

    return(data)
}
