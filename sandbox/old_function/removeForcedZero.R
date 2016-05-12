##' This function removes removes values that should be forced zero.
##'
##' @param data The dataset to be filtered
##' @param forcedZeroKey The index returned by the function
##'     \code{getForcedZeroKey}
##'
##' @return The dataset without index where data are forced to be zero
##' @export

removeForcedZero = function(data, forcedZeroKey){
    dataCopy = copy(data)
    setkeyv(forcedZeroKey, colnames(forcedZeroKey))
    setkeyv(dataCopy, colnames(forcedZeroKey))
    dataCopy[!forcedZeroKey, ]
}
