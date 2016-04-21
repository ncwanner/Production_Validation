selectParentData = function(data,
                            selectedMeatTable,
                            itemVar = "measuredItemCPC",
                            elementVar = "measuredElement"){
    allowedParents = unique(selectedMeatTable[, c("measuredItemParentCPC",
                                                  "measuredElementParent"),
                                              with = FALSE])
    setnames(allowedParents, c(itemVar, elementVar))
    parentData = merge(data, allowedParents, by = colnames(allowedParents))
    parentData    
}
