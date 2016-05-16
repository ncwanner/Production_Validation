##' This function checks whether the animal number in the animal
##' commodity (parent) is in sync with the slaughtered animal in the
##' meat commodity (child).
##'
##' @param commodityTree The mapping table which contains the relationship
##'     between the animal commodity and the meat commodity. Can be retrieved
##'     from \code{getCommodityTree} function.
##' @param animalNumbers The data.table object which contains the animal number
##'     of the animal commodity.
##' @param slaughteredNumbers The data.table object which contains the
##'     slaughtered animal number of the meat commodity.
##' @param returnData logical, whether the data should be returned.
##' @return All three input dataset will be returned as a list if all the the
##'     animal number matches the slaughtered number. Otherwise an error will be
##'     raised.
##'
##' @export
##'

checkSlaughteredSynced = function(commodityTree = getCommodityTree,
                                  animalNumbers = getAllAnimalNumber(),
                                  slaughteredNumbers = getAllSlaughteredNumber(),
                                  returnData = TRUE){
    ## Function to check whether the slaughtered animal and animal
    ## number are in sync.
    ##
    ##
    ## NOTE (Michael): Even 0M- values should be synced!

    animalNumbersCopy = copy(animalNumbers)
    slaughteredNumbersCopy = copy(slaughteredNumbers)

    ## NOTE (Michael): allow.cartesian is set to TRUE here because 1
    ##                 parent can map to multiple children commodity.
    referenceWithAnimalData =
        merge(animalNumbersCopy, commodityTree,
              by = intersect(colnames(animalNumbersCopy),
                             colnames(commodityTree)),
              allow.cartesian = TRUE, all.x = TRUE)
    referenceWithAnimalData[, `:=`(Value, Value * share)]
    setnames(referenceWithAnimalData,
             old = c("Value", "flagObservationStatus", "flagMethod"),
             new = paste0("animal_",
                          c("Value", "flagObservationStatus", "flagMethod")))


    setnames(slaughteredNumbersCopy,
             old = c("measuredItemCPC", "measuredElement"),
             new = c("measuredItemChildCPC", "measuredElementChild"))

    referenceWithSlaughteredData =
        merge(slaughteredNumbersCopy, commodityTree,
              by = intersect(colnames(slaughteredNumbersCopy),
                             colnames(commodityTree)),
              all.x = TRUE)

    setnames(referenceWithSlaughteredData,
             old = c("Value", "flagObservationStatus", "flagMethod"),
             new = paste0("slaughtered_",
                          c("Value", "flagObservationStatus", "flagMethod")))

    referenceWithAllData =
        merge(referenceWithAnimalData, referenceWithSlaughteredData,
              intersect(colnames(referenceWithAnimalData),
                        colnames(referenceWithSlaughteredData)),
              all = TRUE)

    with(referenceWithAllData,
         if(!all(na.omit(animal_Value == slaughtered_Value)))
             stop("Not all values were synced")
         )

    ## Revert the names
    setnames(slaughteredNumbersCopy,
             new = c("measuredItemCPC", "measuredElement"),
             old = c("measuredItemChildCPC", "measuredElementChild"))
    if(returnData)
        return(list(commodityTree = commodityTree,
                    animalNumbers = animalNumbersCopy,
                    slaughteredNumbers = slaughteredNumbersCopy))
}
