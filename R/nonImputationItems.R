nonImputationItems = function(selectedKey, imputationKey){
    selectedItems = selectedKey@dimensions$measuredItemCPC@keys
    imputationItems = imputationKey@dimensions$measuredItemCPC@keys
    nonImputationItems = setdiff(selectedItems, imputationItems)
    nonImputationItems
}
