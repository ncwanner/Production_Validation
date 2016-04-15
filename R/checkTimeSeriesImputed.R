checkTimeSeriesImputed = function(dataToBeSaved, key, valueColumn){
    ## The number of missing values should be either zero or all
    ## missing.
    check = dataToBeSaved[, sum(is.na(.SD[[valueColumn]])) == 0 |
                   sum(is.na(.SD[[valueColumn]])) == .N,
                 by = c(key)]
    unimputedTimeSeries = which(!check$V1)
    if(length(unimputedTimeSeries) > 0){
        stop("Not all time series are imputed")
    }
    dataToBeSaved
}
