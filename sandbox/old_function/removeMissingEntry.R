##' This function filters data to remove missing entries.
##'
##' @param data The dataset
##'
##' @return A data table where flags with 'M' are removed.
##' @export
##'

removeMissingEntry = function(data){
    dataCopy = copy(data)
    dataCopy[!flagObservationStatus == "M", ]
}
