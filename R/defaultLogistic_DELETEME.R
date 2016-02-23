##' Default Logistic Model for the Ensemble.
##' 
##' The model fit is \eqn{ Production = A/(1 + exp(-B(time-C))) }, and
##' fitting is done via non-linear least squares (see ?nls).  If this fit fails
##' to converge, than A is fixed to the maximum Production value and the model
##' is fit via glm().
##' 
##' @note If the midpoint of the logistic regression model is outside the range
##' of the data, then a vector of NA's is returned (to prevent poor
##' extrapolation).
##'
##' @param x A numeric vector to be imputed.
##' @return A numeric vector with the estimated logistic regression model.
##' @export

defaultLogistic = function(x){

    ### Data Quality Checks
    stopifnot(is.numeric(x))
    stopifnot(length(x) > 1)
    
    if(any(x < 0, na.rm = TRUE))
        stop("Found a negative value in logistic fitting!  Value(s) = ",
             paste(x[x<0], collapse = ","))

    time = 1:length(x)
    if(all(is.na(x)))
        return(as.numeric(rep(NA_real_, length(x))))
    #Try most complex model first:
    logisticFit = try(logisticNlsIntercept(x), silent = TRUE)
    #If most complex model failed, try simpler model (intercept=0)
    if(inherits(logisticFit, "try-error"))
        logisticFit = try(logisticNls(x), silent = TRUE)
    #If both non-linear models fail, try simple logistic regression model
    if(inherits(logisticFit, "try-error"))
        logisticFit = try(logisticGlm(x), silent = TRUE)
    logisticFit
}