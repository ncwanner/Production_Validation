##' This function retrieves all the animal number of the animal
##' commodity in the data base.
##'
##' @param animalMeatMapping The mapping table which contains the
##'     relationship between the animal commodity and the meat
##'     commodity. Can be retrieved from \code{getAnimalMeatMapping}
##'     function.
##'
##' @return A data.table containing animal numbers
##'
##' @export

getAllAnimalNumber = function(animalMeatMapping = getAnimalMeatMapping()){
    
    mainKey = getMainKey(swsContext.datasets[[1]]@dimensions$timePointYears@keys)
    mainKey@dimensions$measuredItemCPC@keys =
        animalMeatMapping$measuredItemParentCPC
    mainKey@dimensions$measuredElement@keys =
        animalMeatMapping$measuredElementParent
    
    animalNumbers = GetData(mainKey)
    animalNumbers
}
