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
    ## NOTE (Michael): Test whether the difference is below 1, this is
    ##                 accounting for rounding error.
    if(!all(na.omit(productionDifference < 1))){
        stop("Production is not balanced, the A * Y = P identity is not satisfied")
    }
    dataToBeSaved
}
