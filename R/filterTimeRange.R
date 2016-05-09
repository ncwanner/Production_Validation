##' This functino filters the time dimension of a data
##'
##' @param data The data table object
##' @param firstYear The starting year of the filtering
##' @param lastYear The last year of the filtering
##'
##' @return A filtered dataset
##'
##' @export

filterTimeRange = function(data, firstYear, lastYear){
    dataCopy = copy(data)
    timeFilter = data.table(timePointYears = firstYear:lastYear,
                            key = "timePointYears")
    setkeyv(dataCopy, "timePointYears")
    dataCopy[timeFilter, ]
}
