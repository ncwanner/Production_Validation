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
        currentValueVar = grep(elementCode, valueVars, value = TRUE)
        currentFlagObsVar = grep(elementCode, flagObsVars, value = TRUE)
        currentFlagMethodVar = grep(elementCode, flagMethodVars, value = TRUE)
        dataCopy[is.na(dataCopy[[currentValueVar]]) &
                 is.na(dataCopy[[currentFlagObsVar]]) &
                 is.na(dataCopy[[currentFlagMethodVar]]),
                 `:=`(c(currentValueVar, currentFlagObsVar, currentFlagMethodVar),
                      list(NA, "M", "u"))]
    }
    dataCopy
}
