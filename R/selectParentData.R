##' This function subsets the complete data to include only commodities which
##' are considered a parent.
##'
##' In this case, the animal is the parent of meats.
##'
##' @param data The dataset
##' @param selectedMeatTable Table of the same format as the output of
##'     \code{getAnimalMeatMapping}
##' @param itemVar The column name corresponding item commodity.
##' @param elementVar The column name corresponding to the measured element.
##'
##' @return A dataset where all commodities are parent.
##'
##' @export

selectParentData = function(data,
                            selectedMeatTable,
                            itemVar = "measuredItemCPC",
                            elementVar = "measuredElement"){
    allowedParents = unique(selectedMeatTable[, c("measuredItemParentCPC",
                                                  "measuredElementParent"),
                                              with = FALSE])
    setnames(allowedParents, c(itemVar, elementVar))
    parentData = merge(data, allowedParents, by = colnames(allowedParents))
    parentData
}
