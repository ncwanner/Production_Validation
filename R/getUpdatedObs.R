##' Get Updated Observations
##' 
##' This function takes two data.tables in wide format (i.e. two or three 
##' columns for each element code) and finds all observations which have 
##' different observations.  The values in dataNew are the values choosen to be 
##' saved, and these are returned in a narrow format data.table.
##' 
##' @param dataOld The original data.table, in wide format.
##' @param dataNew The new data.table, in wide format.
##' @param key A vector of column names on which dataOld and dataNew should be 
##'   joined.
##' @param wideVarName The name of the "wide" column.  In other words, if we
##'   have columns with values and flags for each different measuredElement,
##'   then this parameter should be "measuredElement".
##'   
##' @return A data.table with the different observations between dataOld and 
##'   dataNew.
##'   

getUpdatedObs = function(dataOld, dataNew, key, wideVarName){
    compare = merge(dataOld, dataNew, all = TRUE, by = key,
                    suffixes = c("", ".new"))
    wideCols = grep(wideVarName, colnames(dataOld), value = TRUE)
    wideValues = unique(gsub(paste0(".*", wideVarName, "_"),
                             "", wideCols))
    measureVars = unique(gsub(paste0("_.*", wideVarName, ".*"),
                             "", wideCols))
    diffs = lapply(wideValues, function(code){
        measureVar = "Value"
        checkedColumn = paste(measureVar, wideVarName,
                              code, sep = "_")
        # We want to report a difference if neither value is missing and they
        # are different, or if the original value is NA and the new value is
        # not.
        filter = compare[,
            (get(checkedColumn) != get(paste0(checkedColumn, ".new")) |
                is.na(get(checkedColumn))) &
            !is.na(get(paste0(checkedColumn, ".new")))]
        filter[is.na(filter)] = FALSE
        compare[, c(wideVarName) := code]
        selectedCols = c(key, wideVarName,
                         grep(paste0(wideVarName, "_", code, ".new"),
                              colnames(compare), value = TRUE))
        out = compare[filter, selectedCols, with = FALSE]
        compare[, c(wideVarName) := NULL]
        setnames(out, colnames(out), gsub(paste0("_", wideVarName, ".*"), "",
                                          colnames(out)))
        out
    })
    do.call("rbind", diffs)
}