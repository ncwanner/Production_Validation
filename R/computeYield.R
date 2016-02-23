##' Function to compute and update yield
##' 
##' @param data The data.table object containing the data.
##' @param processingParameters A list of the parameters for the production 
##'   processing algorithms.  See defaultProductionParameters() for a starting 
##'   point.
##' @param newObservationFlag The flag which should be placed for computed 
##'   observations as the observation flag.
##' @param newMethodFlag The flag to be used to update the yield method flag 
##'   when imputation occurs.
##' @param flagTable see data(faoswsFlagTable) in \pkg{faoswsFlag}
##' @param unitConversion Yield is computed as (production) / (area) and 
##'   multiplied by unitConversion.  This parameter defaults to 1.
##'   
##' @return The updated data.table.  This is important in the case where the
##'   data is normalized, as the data.table must be cast and reshaped (and thus
##'   can't be modified by reference).
##'   
##' @export
##' 

computeYield = function(data, processingParameters, normalized = FALSE,
                        newObservationFlag = "I", newMethodFlag = "i",
                        flagTable = faoswsFlagTable, unitConversion = 1){

    if(!exists("ensuredProductionData") || !ensuredProductionData)
        ensureProductionInputs(data = data,
                               processingParameters = processingParameters)
    stopifnot(faoswsUtil::checkMethodFlag(newMethodFlag))
    stopifnot(faoswsUtil::checkObservationFlag(newObservationFlag))

    ## Abbreviate processingParameters since it is used alot
    pp = processingParameters

    ## Balance yield values only when they're missing
    missingYield = is.na(data[[pp$yieldValue]]) |
        data[[pp$yieldObservationFlag]] == "M"
    filter = missingYield &
        !is.na(data[[pp$productionValue]]) &
        !is.na(data[[pp$areaHarvestedValue]]) &
        data[[pp$productionObservationFlag]] != "M" &
        data[[pp$areaHarvestedObservationFlag]] != "M"
    data[filter, c(pp$yieldValue) :=
         faoswsUtil::computeRatio(get(pp$productionValue),
                                  get(pp$areaHarvestedValue)) * unitConversion]
    data[filter, c(pp$yieldObservationFlag) := newObservationFlag]
    data[filter, c(pp$yieldMethodFlag) := newMethodFlag]
    ## If yieldValue is still NA, make sure observation flag is "M".  Note:
    ## this can happen by taking 0 production / 0 area.
    data[is.na(get(pp$yieldValue)), c(pp$yieldObservationFlag) := "M"]
    data[is.na(get(pp$yieldValue)), c(pp$yieldMethodFlag) := "u"]
    data[is.na(get(pp$yieldValue)), c(pp$yieldValue) := 0]
    
    return(data)
}
