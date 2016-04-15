checkSlaughteredSynced = function(animalMeatMapping = getAnimalMeatMapping(),
                                  animalNumbers = getAllAnimalNumber(),
                                  slaughteredNumbers = getAllSlaughteredNumber()){
    ## Function to check whether the slaughtered animal and animal
    ## number are in sync.
    ##
    ##
    ## NOTE (Michael): Even 0M- values should be synced!

    animalNumbersCopy = copy(animalNumbers)
    slaughteredNumbersCopy = copy(slaughteredNumbers)
    
    setnames(animalNumbersCopy,
             old = c("measuredItemCPC", "measuredElement"),
             new = c("measuredItemParentCPC", "measuredElementParent"))
    referenceWithAnimalData =
        merge(animalNumbersCopy, animalMeatMapping,
              by = intersect(colnames(animalNumbersCopy),
                             colnames(animalMeatMapping)),
              allow.cartesian = TRUE, all.x = TRUE)

    setnames(referenceWithAnimalData,
             old = c("Value", "flagObservationStatus", "flagMethod"),
             new = paste0("animal_",
                          c("Value", "flagObservationStatus", "flagMethod")))


    setnames(slaughteredNumbersCopy,
             old = c("measuredItemCPC", "measuredElement"),
             new = c("measuredItemChildCPC", "measuredElementChild"))
    
    referenceWithSlaughteredData =
        merge(slaughteredNumbersCopy, animalMeatMapping,
              by = intersect(colnames(slaughteredNumbersCopy),
                             colnames(animalMeatMapping)),
              allow.cartesian = TRUE, all.x = TRUE)

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
    setnames(animalNumbersCopy,
             new = c("measuredItemCPC", "measuredElement"),
             old = c("measuredItemParentCPC", "measuredElementParent"))
    setnames(slaughteredNumbersCopy,
             new = c("measuredItemCPC", "measuredElement"),
             old = c("measuredItemChildCPC", "measuredElementChild"))

    list(animalMeatMapping = animalMeatMapping,
         animalNumbers = animalNumbersCopy,
         slaughteredNumbers = slaughteredNumbersCopy)
}
