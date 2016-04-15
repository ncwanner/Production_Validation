##' This function subsets the animal meat mapping returned by the
##' \code{getAnimalMeatMapping} so that it contains item only in the
##' selected session.
##'
##' @param animalMeatMapping The mapping returned by the
##'     \code{getAnimalMeatMapping}
##' @param context The swsContext.datasets returned by the
##'     \code{GetTestEnvironment}.
##'
##' @return The same animal meat mapping table but with only items
##'     selected by the session.
##'
##' @export
##'

subsetAnimalMeatMapping = function(animalMeatMapping, context){
    selectedItems = context@dimensions$measuredItemCPC@keys
    animalMeatMapping[measuredItemParentCPC %in% selectedItems |
                      measuredItemChildCPC %in% selectedItems, ]
}

