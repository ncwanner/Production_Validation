##' Function to check whether the production itdentity is satisfied.
##'
##' @param dataToBeSaved The data.table to be saved back to the SWS.
##' @param areaVar The column name corresponding to the area harvested.
##' @param yieldVar The column name corresponding to the yield.
##' @param prodVar The column name corresponding to produciton.
##' @param conversion The conversion factor for calculating production.
##' @param returnData logical, whether the data should be returned.
##' @param normalised logical, whether the data is normalised
##' @return If the data satisfy the production identity, then the
##'     original data to be tested is returned. Otherwise, an error is
##'     raised.
##' @export
##'

checkProductionBalanced = function(dataToBeSaved,
                                   areaVar,
                                   yieldVar,
                                   prodVar,
                                   conversion,
                                   returnData = TRUE,
                                   normalised = TRUE){

    dataCopy = copy(data)
    ## Basic checks
    stopifnot(is(dataCopy, "data.table"))
    stopifnot(is(processingParameters, "list"))

    if(normalised){
        dataCopy = denormalise(dataCopy, "measuredElement")
    }
    stopifnot(all(c(areaVar, yieldVar, prodVar) %in% colnames(dataCopy)))

    for(i in seq(areaVar)){
        productionDifference =
            abs(dataCopy[[areaVar[i]]] * dataCopy[[yieldVar[i]]] -
                dataCopy[[prodVar[i]]] * conversion[i])

        ## NOTE (Michael): This is to account for difference due to
        ##                 rounding. The upper bound of the 1e-6 is the
        ##                 rounding performed by the system.
        allowedDifference = max(dataCopy[[areaVar[i]]] * 1e-6, 1)
        if(any(na.omit(productionDifference > allowedDifference))){
            ## if(!all(na.omit(productionDifference < 2))){
            stop("Production is not balanced, the A * Y = P identity is not satisfied")
        }
    }
    if(normalised){
        dataCopy = normalise(dataCopy)
    }

    if(returnData)
        return(dataCopy)
}
