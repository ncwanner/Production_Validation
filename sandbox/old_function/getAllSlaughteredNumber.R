##' This function retrieves all the animal slaughtered number of the
##' meat commodity in the data base.
##'
##' @param animalMeatMapping The mapping table which contains the
##'     relationship between the animal commodity and the meat
##'     commodity. Can be retrieved from \code{getAnimalMeatMapping}
##'     function.
##'
##' @return A data.table containing animal slaughtered numbers
##'
##' @export


getAllSlaughteredNumber = function(animalMeatMapping = getAnimalMeatMapping()){
    
    mainKey = getMainKey(swsContext.datasets[[1]]@dimensions$timePointYears@keys)
    mainKey@dimensions$measuredItemCPC@keys =
        animalMeatMapping$measuredItemChildCPC
    mainKey@dimensions$measuredElement@keys =
        animalMeatMapping$measuredElementChild
    
    slaughteredNumbers = GetData(mainKey)
    slaughteredNumbers
}
