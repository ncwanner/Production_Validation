subsetAnimalMeatMapping = function(animalMeatMapping, context){
    selectedItems = context@dimensions$measuredItemCPC@keys
    animalMeatMapping[measuredItemParentCPC %in% selectedItems |
                      measuredItemChildCPC %in% selectedItems, ]
}

