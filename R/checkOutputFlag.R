##' This is a function to check whether the flags in the data confine
##' to expectation.
##'
##' @param data The data.table object to be checked
##' @param flagObservationStatusColumn The column name corresponding
##'     to the observation status flag.
##' @param flagObservationStatusExpected The value of the observation
##'     status flag expected in the output.
##' @param flagMethodColumn The column name corresponding to the
##'     method flag.
##' @param flagMethodExpected The value of the method flag expected in
##'     the output.
##'
##' @return The original data is returned if all the flag matches
##'     those expected, otherwise an error is raised.
##'
##' @export
##'

checkOutputFlags = function(data,
                            flagObservationStatusColumn = "flagObservationStatus",
                            flagObservationStatusExpected,
                            flagMethodColumn = "flagMethod",
                            flagMethodExpected{
    if(!all(data[[flagObservationStatusColumn]] %in%
            flagObservationStatusExpected))
        stop("Incorrect Observation Flag")
    if(!all(data[[flagMethodColumn]] %in% flagMethodExpected))
        stop("Incorrect Method Flag")
    data
}
