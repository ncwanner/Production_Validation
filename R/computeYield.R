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
    p = processingParameters

    ## Balance yield values only when they're missing
    missingYield = is.na(data[[p$yieldValue]]) |
        data[[p$yieldObservationFlag]] == "M"
    filter = missingYield &
        !is.na(data[[p$productionValue]]) &
        !is.na(data[[p$areaHarvestedValue]]) &
        data[[p$productionObservationFlag]] != "M" &
        data[[p$areaHarvestedObservationFlag]] != "M"
    data[filter, c(p$yieldValue) :=
         faoswsUtil::computeRatio(get(p$productionValue),
                                  get(p$areaHarvestedValue)) * unitConversion]
    data[filter,
         `:=`(c(p$yieldObservationFlag),
              aggregateObservationFlag(get(p$productionObservationFlag),
                                       get(p$areaHarvestedObservationFlag)))]
    data[filter, c(p$yieldMethodFlag) := newMethodFlag]
    ## If yieldValue is still NA, make sure observation flag is "M".  Note:
    ## this can happen by taking 0 production / 0 area.
    data[is.na(get(p$yieldValue)), c(p$yieldObservationFlag) := "M"]
    data[is.na(get(p$yieldValue)), c(p$yieldMethodFlag) := "u"]

    return(data)
}
