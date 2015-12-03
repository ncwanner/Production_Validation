##' Round Results
##' 
##' This function is used to round the production and area harvested values in
##' the production imputation and yield computation modules.
##' 
##' @param x A numeric value to be rounded.
##' 
##' @result A numeric value after the rounding has occurred.
##' 

roundResults = function(x){
    if(is.na(x)){
        return(x)
    }
    stopifnot(x >= 0)
    stopifnot(is(x, "numeric"))
    if(x > 10){
        return(round(x/10)*10)
    } else {
        return(round(x))
    }
}