preProcessing = function(data, params = defaultProcessingParameters()){
    ## Converting year to numeric for modelling
    data[, `:=`(c(params$yearValue), as.numeric(.SD[[params$yearValue]]))]
}
