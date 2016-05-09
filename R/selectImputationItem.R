selectImputationItem = function(selectedKey, imputationKey){
    selectedItems = selectedKey@dimensions$measuredItemCPC@keys
    imputationItems = imputationKey@dimensions$measuredItemCPC@keys
    selectedImputationItems = intersect(selectedItems, imputationItems)
    selectedImputationItems
}
