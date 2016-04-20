##' This function is the reverse of \code{remove0M} which replaces
##' (NA, M) with (0, M)
##'
##'
##' @param data The data.table object containing the values.
##' @param valueVars The value of the observation.
##' @param flagVars The flag of the observation.
##' @param missingFlag The value of the flag which denotes missing value.
##'
##' @return The data object with all the (0, M) replaced with (NA, M)
##' @export
##' 

restore0M = function(data, valueVars, flagVars, missingFlag = "M"){
    dataCopy = copy(data)
    mapply(FUN = function(value, flag){
        dataCopy[which(dataCopy[[flag]] == missingFlag), `:=`(c(value), 0)]
    }, value = valueVars, flag = flagVars)
    dataCopy
}
