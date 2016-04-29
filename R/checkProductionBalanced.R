##' Function to check whether the production itdentity is satisfied.
##'
##' @param dataToBeSaved The data.table to be saved back to the SWS.
##' @param formulaTable The table containing the formula constructed
##'     by \code{constructFormulaTable}
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
    productionDifference =
        abs(dataToBeSaved[[areaVar]] * dataToBeSaved[[yieldVar]] -
            dataToBeSaved[[prodVar]] * conversion)

    ## NOTE (Michael): This is to account for difference due to
    ##                 rounding. The upper bound of the 1e-6 is the
    ##                 rounding performed by the system.
    allowedDifference = dataToBeSaved[[areaVar]] * 1e-6
    if(any(na.omit(productionDifference > allowedDifference))){
    ## if(!all(na.omit(productionDifference < 2))){
        stop("Production is not balanced, the A * Y = P identity is not satisfied")
    }
    dataToBeSaved
}
