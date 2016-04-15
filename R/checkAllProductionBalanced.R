checkAllProductionBalanced = function(dataToBeSaved, formulaTable){
    lapply(unique(dataToBeSaved$measuredItemCPC),
           FUN = function(x){
               cat("Checking formula for commodity: ", x, "\n")
               commodityFormula = formulaTable[which(measuredItemCPC == x), ]
               commodityData = dataToBeSaved[measuredItemCPC == x, ]
               with(commodityFormula,
                    checkProductionBalanced(data = commodityData,
                                            areaVar = input,
                                            yieldVar = productivity,
                                            prodVar = output,
                                            conversion = unitConversion)
                    )
           })
    dataToBeSaved
}
