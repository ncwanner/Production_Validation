
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
