transferParentToChild = function(parentData, commodityTree, selectedMeatTable){
    parentDataWithMapping =
        merge(parentData, commodityTree,
              by = c("geographicAreaM49", "timePointYears", "measuredItemCPC"),
              all.x = TRUE)
    if(any(is.na(parentDataWithMapping$measuredItemChildCPC))){
        warning("Not all child mapped, unmapped children will be ommited! ",
                "Please updated the commodity table so all child are mapped ",
                "for each country, commodity and year combination key")
        parentDataWithMapping =
            parentDataWithMapping[!is.na(measuredItemChildCPC), ]
    }

    parentDataWithMapping[, `:=`(c("Value", "share"), list(Value * share, NULL))]
    parentDataWithMapping[, `:=`(c("measuredItemCPC", "extractionRate"),
                                 list(NULL, NULL))]
    setnames(parentDataWithMapping, "measuredItemChildCPC", "measuredItemCPC")

    
    childElementTable =
        selectedMeatTable[, c("measuredItemChildCPC", "measuredElementChild"),
                          with = FALSE]
    setnames(childElementTable, "measuredItemChildCPC", "measuredItemCPC")
    setkeyv(childElementTable, cols = "measuredItemCPC")
    setkeyv(parentDataWithMapping, cols = "measuredItemCPC")
    parentDataWithMapping[childElementTable,
                          `:=`(measuredElement, i.measuredElementChild)]
    setcolorder(parentDataWithMapping, neworder = colnames(parentData))
    parentDataWithMapping
}
