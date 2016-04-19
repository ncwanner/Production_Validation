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
