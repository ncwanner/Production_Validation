postProcessing = function(data, params = defaultProcessingParameters()){
        ## Converting year back to database
    data[, `:=`(c(params$yearValue), as.character(.SD[[params$yearValue]]))]
}

