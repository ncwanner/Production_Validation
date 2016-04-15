getAllAnimalNumber = function(animalMeatMapping = getAnimalMeatMapping()){
    
    mainKey = getMainKey(swsContext.datasets[[1]]@dimensions$timePointYears@keys)
    mainKey@dimensions$measuredItemCPC@keys =
        animalMeatMapping$measuredItemParentCPC
    mainKey@dimensions$measuredElement@keys =
        animalMeatMapping$measuredElementParent
    
    animalNumbers = GetData(mainKey)
    animalNumbers
}
