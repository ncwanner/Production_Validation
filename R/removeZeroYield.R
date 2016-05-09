removeZeroYield = function(data, yieldValue, yieldObsFlag, yieldMethodFlag){
    dataCopy = copy(data)
    dataCopy[get(yieldValue) == 0,
             `:=`(c(yieldValue, yieldObsFlag, yieldMethodFlag),
                  list(NA, "M", "u"))]
    dataCopy
}
    
