##' Function to compute production when new area harvested and yield are given.
##' 
##' @param data The data.table object containing the data.
##' @param processingParameters A list of the parameters for the production 
##'   processing algorithms.  See defaultProcessingParameters() for a starting 
##'   point.
##' @param newObservationFlag The flag which should be placed for computed
##'   observations as the observation flag.
##' @param newMethodFlag The flag which should be placed for computed
##'   observations as the method flag.
##' @param unitConversion Yield is computed as (production) / (area) and 
##'   multiplied by unitConversion.  This parameter defaults to 1.
##'   
##' @export
##' 

balanceProduction = function(data, processingParameters,
                             newObservationFlag = "I", newMethodFlag = "i",
                             unitConversion = 1){

    ### Data Quality Checks
    
    ### Type "p" instead of "processingParameters"
    p = processingParameters
    data[get(p$productionObservationFlag) == "M" &
         get(p$productionMethodFlag) == "u",
         c(p$productionValue) := NA]
        
    ### Impute only when area and yield are available and production isn't
    filter = data[,!is.na(get(p$areaHarvestedValue)) & # area is available
                    is.na(get(p$productionValue)) &    # production is missing
                   !is.na(get(p$yieldValue))]          # yield is missing

    data[filter, c(p$productionValue) :=
             sapply(get(p$areaHarvestedValue) * get(p$yieldValue) /
                        unitConversion, FUN = roundResults)]
    data[filter, c(p$productionObservationFlag) := newObservationFlag]
    ## Wrap last call in invisible() so no data.table is returned
    invisible(data[filter, c(p$productionMethodFlag) := newMethodFlag])
}
