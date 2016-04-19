##' This function expands the current selection in the session to
##' include all necessary meat.
##'
##' @param oldKey The current key used in the session
##' @param selectedMeatTable The data table of the same format returned by
##'     the function \code{getAnimalMeatMapping}, but containing the
##'     meat selection required.
##'
##' @return A new DatasetKey object expanded to include the meat
##'     selection specified.
##'
##' @export
##'

expandMeatSessionSelection = function(oldKey, selectedMeatTable){
    rowsIncluded =
        selectedMeatTable[, measuredItemParentCPC %in%
                       oldKey@dimensions$measuredItemCPC@keys |
                       measuredItemChildCPC %in%
                       oldKey@dimensions$measuredItemCPC@keys]
    requiredMeats =
        selectedMeatTable[rowsIncluded, c(measuredItemParentCPC, measuredItemChildCPC)]
    oldKey@dimensions[[itemVar]]@keys = requiredMeats
    if(length(oldKey@dimensions$measuredItemCPC@keys) == 0){
        stop("No meat/animal commodities are in the session, and thus this ",
             "module has nothing to do.")
    }

    ## Create a copy to update the key
    newKey = oldKey
    ## Update the measuredElements
    newKey@dimensions[[elementVar]]@keys =
        unique(selectedMeatTable[rowsIncluded,
                            c(measuredElementParent, measuredElementChild)])
    
    ## Adjust the years based on the passed information:
    newKey@dimensions[[yearVar]]@keys =
        as.character(firstDataYear:lastYear)

    ## Include all countries, since all data is required for the imputation
    countryCodes = GetCodeList("agriculture", "aproduction", "geographicAreaM49")
    newKey@dimensions[[areaVar]]@keys = countryCodes[type == "country", code]
    newKey
}
