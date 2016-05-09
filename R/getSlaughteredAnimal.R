##' Function to delete all other elements but only retain elements corresponding
##' to slaughtered animal
##'
##' @param data The dataset
##' @param formulaTuples The output of \code{getYieldFormula}
##'
##' @return A data table where non slaughtered animal elements are removed
##' @export

getSlaughteredAnimal = function(data, formulaTuples){
    dataCopy = copy(data)
    ## Delete productivity columns
    dataCopy[, paste0(c("Value", "flagObservationStatus", "flagMethod"),
                      "_measuredElement_",
                      formulaTuples$productivity) := NULL]
    ## Delete output columns
    dataCopy[, paste0(c("Value", "flagObservationStatus", "flagMethod"),
                      "_measuredElement_",
                      formulaTuples$output) := NULL]
    ## Rename input column to measured element and delete all
    ## remaining column
    dataCopy[, measuredElement := formulaTuples$input]
    setnames(dataCopy, colnames(dataCopy),
             gsub("_measuredElement_.*", "", colnames(dataCopy)))
    dataCopy
}
