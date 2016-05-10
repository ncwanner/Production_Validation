##' Ignore Zero Series
##'
##' This function assumes a data format of three dimension columns
##' (measuredItemCPC, geographicAreaM49, and timePointYears) followed by 9
##' Value/flagObservationStatus/flagMethod columns.  It determines, for each
##' measuredItemCPC and geographicAreaM49 pair, the most recent value (ignoring
##' missing flags).  In case of ties, maximum values are used.  If this most
##' recent value is zero, the series is assumed to have stopped and is removed
##' from the imputation process.
##'
##' @param d A data.table with columns as described above.
##' @param missingObsFlag The observation flag for missing observations.
##' @param missingMetFlag The method flag for missing observations.
##' @param firstImputeYear What is the first year for the imputation?
##'
##' @return Nothing is returned, but the passed dataset is filtered to the
##'   correct rows.
##'
##' @export

ignoreZeroSeries = function(d, missingObsFlag = "M", missingMetFlag = "u",
                            firstImputeYear = 2010){
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
    suppressWarnings({ # Avoid 'Returning -Inf when no value found' warning
        lastVal[!is.na(Value) & timePointYears <= firstImputeYear, maxYear := max(timePointYears),
                by = c("geographicAreaM49", "measuredItemCPC")]
    })
    if("maxYear" %in% colnames(lastVal)){
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
        postfix = gsub("Value", "", valCols)

        ## If any value is 0M-, stop the series
        filter = lapply(postfix, function(code){
            d[, (is.na(get(paste0("Value", code))) |
                 get(paste0("Value", code)) == 0) &
                sapply(get(paste0("flagObservationStatus", code)), identical, y = "M") &
                sapply(get(paste0("flagMethod", code)), identical, y = "-")]
        })
        ## May be a better way to do this:
        filter = apply(do.call("cbind", filter), 1, any)
        d[, stopSeries := stopSeries | filter]

        sapply(postfix, function(p){
            d[(stopSeries),
              c(paste0(c("Value", "flagObservationStatus", "flagMethod"), p)) :=
              list(0, "E", "-")]
            d[is.na(get(paste0("Value", p))) & !(stopSeries),
              c(paste0(c("Value", "flagObservationStatus", "flagMethod"), p)) :=
              list(0, "M", "u")]
        })
        ##     sapply(obsFlagCols, function(colname){
        ##         d[is.na(get(colname)) & !stopSeries, c(colname) := "M"]})
        ##     sapply(metFlagCols, function(colname){
        ##         d[is.na(get(colname)) & stopSeries , c(colname) := "-"]})
        ##     sapply(metFlagCols, function(colname){
        ##         d[is.na(get(colname)) & !stopSeries, c(colname) := "u"]})
        ##         d[is.na(get(colname)), c(colname) := 0]})
    }
    d[, stopSeries := NULL]
}
