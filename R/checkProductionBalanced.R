##' Function to check whether the production itdentity is satisfied.
##'
##' @param dataToBeSaved The data.table to be saved back to the SWS.
##' @param areaVar The column name corresponding to the area harvested.
##' @param yieldVar The column name corresponding to the yield.
##' @param prodVar The column name corresponding to produciton.
##' @param conversion The conversion factor for calculating production.
##'
##' @return If the data satisfy the production identity, then the
##'     original data to be tested is returned. Otherwise, an error is
##'     raised.
##' @export
##'

checkProductionBalanced = function(dataToBeSaved,
                                   areaVar,
                                   yieldVar,
                                   prodVar,
                                   conversion){
    stopifnot(all(c(areaVar, yieldVar, prodVar) %in% colnames(dataToBeSaved)))

    for(i in seq(areaVar)){
        productionDifference =
            abs(dataToBeSaved[[areaVar[i]]] * dataToBeSaved[[yieldVar[i]]] -
                dataToBeSaved[[prodVar[i]]] * conversion[i])

        ## NOTE (Michael): This is to account for difference due to
        ##                 rounding. The upper bound of the 1e-6 is the
        ##                 rounding performed by the system.
        allowedDifference = max(dataToBeSaved[[areaVar[i]]] * 1e-6, 1)
        if(any(na.omit(productionDifference > allowedDifference))){
            ## if(!all(na.omit(productionDifference < 2))){
            stop("Production is not balanced, the A * Y = P identity is not satisfied")
        }
    }
    dataToBeSaved
}
