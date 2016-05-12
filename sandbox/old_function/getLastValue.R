##' Get Last Value
##'
##' @param d A data.table with columns as described above.
##' @param missingObsFlag The observation flag for missing observations.
##' @param missingMetFlag The method flag for missing observations.
##'
##' @return Nothing is returned, but the passed dataset is filtered to the
##'   correct rows.
##'
##' @export

getLastValue = function(d, missingObsFlag = "M", missingMetFlag = "u"){
    lastVal = copy(d) # Remove the link to passed data.table
    valCols = grep("Value_", colnames(d), value = TRUE)
    obsFlagCols = grep("flagObservation", colnames(d), value = TRUE)
    metFlagCols = grep("flagMethod", colnames(d), value = TRUE)
    lastVal = melt(lastVal, id.vars = c("geographicAreaM49", "measuredItemCPC",
                                        "timePointYears"),
                   measure.vars = list(valCols, obsFlagCols, metFlagCols))
    setnames(lastVal, c("value1", "value2", "value3"),
             c("Value", "obsFlag", "metFlag"))
    lastVal = lastVal[!(obsFlag == "M" & metFlag == "u"), ] # Also filters out NA flags
    lastVal[, maxYear := max(timePointYears),
            by = c("geographicAreaM49", "measuredItemCPC")]
    lastVal = lastVal[timePointYears == maxYear, ]
    lastVal = lastVal[, list(lastValue = max(Value, na.rm = TRUE)),
                      by = c("geographicAreaM49", "measuredItemCPC")]
    lastVal[, stopSeries := is.na(lastValue) | lastValue <= 0]
    d[lastVal, stopSeries := stopSeries,
      on = c("geographicAreaM49", "measuredItemCPC")]
    d[is.na(stopSeries), stopSeries := TRUE] # If we have NAs from merging
    valCols = grep("Value_", colnames(d), value = TRUE)
    obsFlagCols = grep("flagObservation", colnames(d), value = TRUE)
    metFlagCols = grep("flagMethod", colnames(d), value = TRUE)

    sapply(obsFlagCols, function(colname){
        d[is.na(get(colname)) & stopSeries , c(colname) := "F"]})
    sapply(obsFlagCols, function(colname){
        d[is.na(get(colname)) & !stopSeries, c(colname) := "M"]})
    sapply(metFlagCols, function(colname){
        d[is.na(get(colname)) & stopSeries , c(colname) := "-"]})
    sapply(metFlagCols, function(colname){
        d[is.na(get(colname)) & !stopSeries, c(colname) := "u"]})
    sapply(valCols, function(colname){
        d[is.na(get(colname)), c(colname) := 0]})
}
