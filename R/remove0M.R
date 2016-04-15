##' This function replaces value which are zero and has flag
##' representing missing values with NA.
##'
##' NOTE (Michael): This function is similar to the function in the
##'                 \pkg{faoswsUtil} package, but enables multiple
##'                 value and flag pairs and also returning the data
##'                 object. This function should replace the original
##'                 one in the \pkg{faoswsUtil} package
##'
##' @param data The data.table object containing the values.
##' @param valueVars The value of the observation.
##' @param flagVars The flag of the observation.
##' @param missingFlag The value of the flag which denotes missing value.
##'
##' @return The data object with all the (0, M) replaced with (NA, M)
##' @export
##' 

remove0M = function(data, valueVars, flagVars, missingFlag = "M"){
    dataCopy = copy(data)
    mapply(FUN = function(value, flag){
        dataCopy[which(dataCopy[[flag]] == missingFlag), `:=`(c(value), NA_real_)]
    }, value = valueVars, flag = flagVars)
    dataCopy
}
