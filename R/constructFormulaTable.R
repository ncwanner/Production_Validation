##' This function construct a formula table
##'
##' The formula table is used by the \code{checkAllProductionBalanced}
##' function.
##'
##' @param formulaTuples The containing the formula relationship, this
##'     can be obtained from the \code{getYieldFormula} function.
##' @param formulaPrefix This table contains the prefix which is used
##'     to contruct the colnames. Can be obtained from the
##'     \code{getFormulaPrefix} function.
##' @param whichPrefix This parameter determines which of the three
##'     prefix in the formula prefix table should be concatenate in
##'     the name.
##'
##' @return A formula table
##'
##' @export
##'

constructFormulaTable = function(formulaTuples, formulaPrefix,
                                 whichPrefix = c("valuePrefix", "flagObsPrefix",
                                                 "flagMethodPrefix")){
    whichPrefix = match.arg(whichPrefix)
    formulaTable = copy(formulaTuples)
    formulaTable[, `:=`(c("input", "productivity", "output"),
                        lapply(.SD[, c("input", "productivity", "output"),
                                   with = FALSE],
                               FUN = function(x){
                                   paste0(formulaPrefix[[whichPrefix]], x)
                               }))]
    formulaTable
}
