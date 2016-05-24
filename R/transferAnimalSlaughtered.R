##' Function to transfer animal slaughtered from animal to meat or back from
##' meat to animals.
##'
##' @param meatData The animal slaughtered data from meat commodity.
##' @param animalData The animal slaughtered data from animal commodity.
##' @param parentToChild logical, if true, slaughtered animal are transferred
##'     from animal commodity to meat, otherwise the otherway around.
##' @param mappingTable The mapping table between the parent and the child.
##'
##' @return The transferred data
##' @export

transferAnimalSlaughtered = function(meatData,
                                     animalData,
                                     mappingTable,
                                     parentToChild = TRUE){

    requiredColumn = c("geographicAreaM49",
                       "measuredItemCPC",
                       "measuredElement",
                       "timePointYears",
                       "Value",
                       "flagObservationStatus",
                       "flagMethod")

    ensureDataInput(data = meatData,
                    requiredColumn = requiredColumn,
                    returnData = FALSE)
    ensureDataInput(data = animalData,
                    requiredColumn = requiredColumn,
                    returnData = FALSE)

    meatDataCopy = copy(meatData)
    animalDataCopy = copy(animalData)

    setnames(x = meatDataCopy,
             old = c("measuredItemCPC", "measuredElement",
                     "Value", "flagObservationStatus", "flagMethod"),
             new = c("measuredItemChildCPC", "measuredElementChild",
                     paste0(c("Value", "flagObservationStatus", "flagMethod"),
                            "_child")))
    setnames(x = animalDataCopy,
             old = c("measuredItemCPC", "measuredElement",
                     "Value", "flagObservationStatus", "flagMethod"),
             new = c("measuredItemParentCPC", "measuredElementParent",
                     paste0(c("Value", "flagObservationStatus", "flagMethod"),
                            "_parent")))

    mergeMeatCol = intersect(colnames(meatDataCopy),
                             colnames(mappingTable))
    meatDataMapped = merge(meatDataCopy, mappingTable,
                           by = mergeMeatCol, all.y = TRUE)

    mergeAnimalCol = intersect(colnames(animalDataCopy),
                               colnames(mappingTable))
    animalDataMapped = merge(animalDataCopy, mappingTable,
                             by = mergeAnimalCol, all.y = TRUE)


    mergeAnimalMeatCol =
        intersect(colnames(meatDataMapped), colnames(animalDataMapped))
    animalMeatMerged =
        merge(meatDataMapped, animalDataMapped,
              by = mergeAnimalMeatCol, all = TRUE)

    ## Need to check what can be copied.
    if(parentToChild){
        animalMeatMerged[, `:=`(c("measuredItemCPC",
                                  "measuredElement",
                                  "Value",
                                  "flagObservationStatus",
                                  "flagMethod"),
                                list(measuredItemChildCPC,
                                     measuredElementChild,
                                     Value_parent,
                                     flagObservationStatus_parent,
                                     "c"))]
    } else {
        animalMeatMerged[, `:=`(c("measuredItemCPC",
                                  "measuredElement",
                                  "Value",
                                  "flagObservationStatus",
                                  "flagMethod"),
                                list(measuredItemParentCPC,
                                     measuredElementParent,
                                     Value_child,
                                     flagObservationStatus_child,
                                     "c"))]
    }


    dataToBeReturned =
        subset(animalMeatMerged,
               select = requiredColumn)


    dataToBeReturned
}
