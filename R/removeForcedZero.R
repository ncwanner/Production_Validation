removeForcedZero = function(data, forcedZeroKey){
    dataCopy = copy(data)
    setkeyv(forcedZeroKey, colnames(forcedZeroKey))
    setkeyv(dataCopy, colnames(forcedZeroKey))
    dataCopy[!forcedZeroKey, ]
}
