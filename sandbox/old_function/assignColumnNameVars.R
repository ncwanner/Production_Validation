##' Assign Column Name Variables
##'
##' This function acts as a helper function by assigning the column names to the
##' global environment.  This makes code much easier to read as it avoids the
##' use of continual get() statements within data.table.
##'
##' @param prefixTuples See the return object from getProductionData.
##' @param formulaTuples See the return object from getProductionData.
##'
##' @return No value is returned, but many global variables are assigned.
##'
##' @export

assignColumnNameVars = function(prefixTuples, formulaTuples){
    assign("productionValue",
        paste0(prefixTuples$valuePrefix,
               formulaTuples$output),
           envir = .GlobalEnv)
    assign("productionObservationFlag",
        paste0(prefixTuples$flagObsPrefix,
               formulaTuples$output),
           envir = .GlobalEnv)
    assign("productionMethodFlag",
        paste0(prefixTuples$flagMethodPrefix,
               formulaTuples$output),
           envir = .GlobalEnv)
    assign("areaHarvestedValue",
        paste0(prefixTuples$valuePrefix,
               formulaTuples$input),
           envir = .GlobalEnv)
    assign("areaHarvestedObservationFlag",
        paste0(prefixTuples$flagObsPrefix,
               formulaTuples$input),
           envir = .GlobalEnv)
    assign("areaHarvestedMethodFlag",
        paste0(prefixTuples$flagMethodPrefix,
               formulaTuples$input),
           envir = .GlobalEnv)
    assign("yieldValue",
        paste0(prefixTuples$valuePrefix,
               formulaTuples$productivity),
           envir = .GlobalEnv)
    assign("yieldObservationFlag",
        paste0(prefixTuples$flagObsPrefix,
               formulaTuples$productivity),
           envir = .GlobalEnv)
    assign("yieldMethodFlag",
        paste0(prefixTuples$flagMethodPrefix,
               formulaTuples$productivity),
           envir = .GlobalEnv)
}
