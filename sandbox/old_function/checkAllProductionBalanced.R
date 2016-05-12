##' Function to check whether the production itdentity is satisfied.
##'
##' This is a wrapper function for the function
##' \code{checkProductionBalanced} for multiple formula. Formula for
##' crops and meat corresponds to different elements, and this wrapper
##' enables one to check all the formulas specified in the formula
##' table.
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

checkAllProductionBalanced = function(dataToBeSaved, formulaTable){
    invisible(
        lapply(unique(dataToBeSaved$measuredItemCPC),
               FUN = function(x){
                   cat("Checking formula for commodity: ", x, "\n")
                   commodityFormula = formulaTable[which(measuredItemCPC == x), ]
                   commodityData = dataToBeSaved[measuredItemCPC == x, ]
                   with(commodityFormula,
                        checkProductionBalanced(dataToBeSaved = commodityData,
                                                areaVar = input,
                                                yieldVar = productivity,
                                                prodVar = output,
                                                conversion = unitConversion)
                        )
               })
        )
    dataToBeSaved
}
