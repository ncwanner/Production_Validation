remove0M = function(data, valueVars, flagVars, missingFlag = "M"){
    dataCopy = copy(data)
    mapply(FUN = function(value, flag){
        dataCopy[which(dataCopy[[flag]] == missingFlag), `:=`(c(value), NA_real_)]
    }, value = valueVars, flag = flagVars)
    dataCopy
}
