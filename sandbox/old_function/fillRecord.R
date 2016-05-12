##' This function replaces non-existing records with missing records.
##'
##' Occastionally the database may return records where all three columns
##' (value, observation flag, method flag) are all NA and is a non-existing
##' record in the database; this can also be created when denormalising data. We
##' will replace is with a missing record representation where (Value = NA,
##' flagObservationStatus = "M" and flagMethod = "u").
##'
##' @param data The dataset
##' @param valuePattern The regular expression to capture the value column.
##' @param flagObsPattern The regular expression to capture the observation
##'     status flag column.
##' @param flagMethodPattern The regular expression to capture the method flag
##'     column.
##'
##' @return dataset with identical dimension but creates missing record in place
##'     of non-existing record.
##'
##' @export
##'

fillRecord = function(data,
                      valuePattern = "Value",
                      flagObsPattern = "flagObservationStatus",
                      flagMethodPattern = "flagMethod"){
    dataCopy = copy(data)
    elementCodes = gsub("[^0-9]", "",
                        grep("[0-9]{4}", colnames(dataCopy), value = TRUE))
    valueVars = grep(valuePattern, colnames(dataCopy), value = TRUE)
    flagObsVars = grep(flagObsPattern, colnames(dataCopy), value = TRUE)
    flagMethodVars = grep(flagMethodPattern, colnames(dataCopy), value = TRUE)

    for(elementCode in elementCodes){
        currentValueVar = grep(paste0(elementCode, "$"), valueVars, value = TRUE)
        currentFlagObsVar = grep(paste0(elementCode, "$"),
                                 flagObsVars, value = TRUE)
        currentFlagMethodVar = grep(paste0(elementCode, "$"),
                                    flagMethodVars, value = TRUE)
        dataCopy[is.na(dataCopy[[currentValueVar]]) &
                 is.na(dataCopy[[currentFlagObsVar]]) &
                 is.na(dataCopy[[currentFlagMethodVar]]),
                 `:=`(c(currentValueVar, currentFlagObsVar, currentFlagMethodVar),
                      list(NA, "M", "u"))]
    }
    dataCopy
}
