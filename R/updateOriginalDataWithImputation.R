updateOriginalDataWithImputation = function(originalData, imputedData,
                                            yieldElementCode,
                                            prodElementCode){
    
    ## Add in yield estimates back to original data
    toMerge = imputedData[measuredElement == yieldElementCode, ]
    cols = c("Value", "flagObservationStatus", "flagMethod")
    newCols = paste0(cols, "_measuredElement_",
                     yieldElementCode)
    setnames(toMerge, cols, newCols)
    updatedData = merge(originalData, toMerge, all = TRUE,
                        suffixes = c("", ".new"),
                        by = c("geographicAreaM49", "timePointYears"))
    
    for(column in newCols){
        updatedData[!is.na(get(paste0(column, ".new"))), c(column) := 
                    get(paste0(column, ".new"))]
    }
    updatedData[, c(paste0(newCols, ".new"),
                    "measuredElement") := NULL]
    
    ## Add in production estimates back to original data
    toMerge = imputedData[measuredElement == prodElementCode, ]
    newCols = paste0(cols, "_measuredElement_", prodElementCode)
    setnames(toMerge, cols, newCols)
    updatedData = merge(updatedData, toMerge, all = TRUE,
                        suffixes = c("", ".new"),
                        by = c("geographicAreaM49", "timePointYears"))
    for(column in newCols){
        updatedData[!is.na(get(paste0(column, ".new"))), c(column) := 
                    get(paste0(column, ".new"))]
    }
    updatedData[, c(paste0(newCols, ".new"),
                    "measuredElement") := NULL]

}
