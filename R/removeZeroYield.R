##' This function removes yield that bears the value zero and replace with
##' missing value.
##'
##' By definition, yield can not be zero. When both area harvested and
##' production are zero, it is a missing value.
##'
##' @param data The dataset
##' @param yieldValue The column corresponding to the value of yield
##' @param yieldObsFlag The column corresponding to the observation status flag
##'     of yield
##' @param yieldMethodFlag The column corresponding to the method flag of yield
##'
##' @return A data table where all entries with zero yield are replaced with NA.
##' @export

removeZeroYield = function(data, yieldValue, yieldObsFlag, yieldMethodFlag){
    dataCopy = copy(data)
    dataCopy[get(yieldValue) == 0,
             `:=`(c(yieldValue, yieldObsFlag, yieldMethodFlag),
                  list(NA, "M", "u"))]
    dataCopy
}

