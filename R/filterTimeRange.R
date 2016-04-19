filterTimeRange = function(data, firstYear, lastYear){
    dataCopy = copy(data)
    timeFilter = data.table(timePointYears = firstYear:lastYear,
                            key = "timePointYears")
    setkeyv(dataCopy, "timePointYears")
    dataCopy[timeFilter, ]
}
