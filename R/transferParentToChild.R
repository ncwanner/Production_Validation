##' The function transfer values from parent to child
##'
##' @param parentData The data containing the values of the parent
##' @param commodityTree The commodity tree returned by \code{getCommodityTree}.
##' @param selectedMeatTable Table of the same format as the output of
##'     \code{getAnimalMeatMapping}.
##'
##' @return A dataset with the codes of children items while values updated from
##'     parents.
##' @export


transferParentToChild = function(parentData, commodityTree, selectedMeatTable){

    ## Cartesian mapping is allowed as each parent has multiple child.
    parentDataWithMapping =
        merge(parentData, commodityTree,
              by = c("geographicAreaM49", "timePointYears", "measuredItemCPC"),
              all.x = TRUE,
              allow.cartesian = TRUE)
    if(any(is.na(parentDataWithMapping$measuredItemChildCPC))){
        ## NOTE (Michael): After checking the validity of the commodity table,
        ##                 we wil change the warning to an error.
        ##
        ## TODO (Michael): Check the wild card in the commodity tree.
        ##
        ## missingTable = parentDataWithMapping[is.na(measuredItemChildCPC), ]
        ## stop("Not all child mapped,",
        ##      "Please updated the commodity table so all child are mapped ",
        ##      "for each country, commodity and year combination key",
        ##      print(missingTable, NROW(missingTable)))
        warning("Not all child mapped, unmapped children will be ommited! ",
                "Please updated the commodity table so all child are mapped ",
                "for each country, commodity and year combination key")
        parentDataWithMapping =
            parentDataWithMapping[!is.na(measuredItemChildCPC), ]
    }

    ## TODO (Michael): Need to check for the observation and method flags
    parentDataWithMapping[, `:=`(c("Value"), list(Value * share))]
    parentDataWithMapping[, `:=`(c("measuredItemCPC", "extractionRate", "share"),
                                 list(NULL, NULL, NULL))]
    setnames(parentDataWithMapping, "measuredItemChildCPC", "measuredItemCPC")


    childElementTable =
        selectedMeatTable[, c("measuredItemChildCPC", "measuredElementChild"),
                          with = FALSE]
    setnames(childElementTable, "measuredItemChildCPC", "measuredItemCPC")
    setkeyv(childElementTable, cols = "measuredItemCPC")
    setkeyv(parentDataWithMapping, cols = "measuredItemCPC")
    parentDataWithMapping[childElementTable,
                          `:=`(measuredElement, i.measuredElementChild)]
    setcolorder(parentDataWithMapping, neworder = colnames(parentData))
    parentDataWithMapping
}
