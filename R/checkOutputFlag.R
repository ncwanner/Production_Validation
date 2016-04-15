checkOutputFlags = function(data, flagObservationStatusColumn,
                      flagObservationStatusExpected, flagMethodColumn,
                      flagMethodExpected){
    if(!all(data[[flagObservationStatusColumn]] %in%
            flagObservationStatusExpected))
        stop("Incorrect Observation Flag")
    if(!all(data[[flagMethodColumn]] %in% flagMethodExpected))
        stop("Incorrect Method Flag")
    data
}
