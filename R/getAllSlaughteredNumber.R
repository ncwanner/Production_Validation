getAllSlaughteredNumber = function(animalMeatMapping = getAnimalMeatMapping()){
    
    mainKey = getMainKey(swsContext.datasets[[1]]@dimensions$timePointYears@keys)
    mainKey@dimensions$measuredItemCPC@keys =
        animalMeatMapping$measuredItemChildCPC
    mainKey@dimensions$measuredElement@keys =
        animalMeatMapping$measuredElementChild
    
    slaughteredNumbers = GetData(mainKey)
    slaughteredNumbers
}
