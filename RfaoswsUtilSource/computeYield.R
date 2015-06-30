##' Function to compute and update yield
##' 
##' @param data The data.table object containing the data.
##' @param productionValue The column name corresponding to production value.
##' @param productionObservationFlag The column name corresponding to the 
##'   observation flag of production.
##' @param areaHarvestedValue The column name corresponding to area harvested 
##'   value.
##' @param areaHarvestedObservationFlag The column name corresponding to the 
##'   observation flag of area harvested.
##' @param yieldValue The columne name corresponding to yield value.
##' @param yieldObservationFlag The column name corresponding to the observation
##'   flag of yield.
##' @param yieldMethodFlag The column name corresponding to the method flag for 
##'   yield.
##' @param newObservationFlag The character value that should be placed in the 
##'   yieldMethodFlag column when yield is computed.
##' @param newMethodFlag The character value that should be placed in the 
##'   yieldMethodFlag column when yield is computed.
##' @param flagTable Currently not used.  Was originally implemented for flag
##'   aggregation, but the decision was to alway have yield written as "I"/"i".
##' @param unitConversion yield is computed as (production)/(area harvested)* 
##'   (unit conversion).
##'   
##' @export
##' 

computeYield = function(data, productionValue, productionObservationFlag,
    areaHarvestedValue, areaHarvestedObservationFlag, yieldValue,
    yieldObservationFlag, yieldMethodFlag, newObservationFlag = "I",
    newMethodFlag = "i", flagTable, unitConversion = 1){

    if(!yieldValue %in% colnames(data))
        data[, c(yieldValue) := NA]
    if(!yieldObservationFlag %in% colnames(data))
        data[, c(yieldObservationFlag) := NA]
    if(!yieldMethodFlag %in% colnames(data))
        data[, c(yieldMethodFlag) := NA]
    
    setnames(x = data,
             old = c(productionValue, productionObservationFlag,
                     areaHarvestedValue, areaHarvestedObservationFlag,
                     yieldValue, yieldObservationFlag, yieldMethodFlag),
             new = c("productionValue", "productionObservationFlag",
                 "areaHarvestedValue", "areaHarvestedObservationFlag",
                 "yieldValue", "yieldObservationFlag", "yieldMethodFlag"))

    ## Only compute a value if the yield is "updateable", i.e. only if
    ## production and area harvested are not missing.
    updateable = data[, !is.na(productionValue) && !is.na(areaHarvestedValue) &&
                          areaHarvestedValue > 0]
    data[updateable, yieldValue :=
         computeRatio(productionValue, areaHarvestedValue) * unitConversion]
    data[updateable, yieldObservationFlag := newObservationFlag]
    data[updateable, yieldMethodFlag := newMethodFlag]
    
    ## If not updateable, assign a missing value/flag:
    data[!updateable, yieldValue := 0]
    data[!updateable, yieldObservationFlag := "M"]
    data[!updateable, yieldMethodFlag := "n"]

    setnames(x = data,
             old = c("productionValue", "productionObservationFlag",
                 "areaHarvestedValue", "areaHarvestedObservationFlag",
                 "yieldValue", "yieldObservationFlag", "yieldMethodFlag"),
             new = c(productionValue, productionObservationFlag,
                 areaHarvestedValue, areaHarvestedObservationFlag,
                 yieldValue, yieldObservationFlag, yieldMethodFlag))
}
