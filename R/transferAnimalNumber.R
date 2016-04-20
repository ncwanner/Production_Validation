##' This function transfers the animal number element from the parent
##' commodity (e.g. cattle) to the animal slaughtered element of the
##' child commodity (e.g. cattle meat).
##'
##' This procedure is to ensure the two element are in synchronisation
##' as required.
##'
##' @param data A data.table object containing the data.
##' @param selectedMeat The data table of the same format returned by
##'     the function \code{getAnimalMeatMapping}, but containing the
##'     meat selection required.
##'
##' @return A data.table object of the same dimension but with the
##'     values of the slaughtered animal updated by the animal number.
##'
##' @export
##'

transferAnimalNumber = function(data, selectedMeat,
                                areaVar = "geographicAreaM49",
                                itemVar = "measuredItemCPC",
                                elementVar = "measuredElement",
                                yearVar = "timePointYears"){
    dataCopy = copy(data)
    ## Step 1. Transfer down the slaughtered animal numbers from the
    ##         animal (parent) commodity to the meat (child) commodity.

    ## Remove missing values, as we don't want to copy those.
    parentData = dataCopy[!flagObservationStatus == "M" &
                          measuredItemCPC %in% selectedMeat$measuredItemParentCPC &
                          timePointYears <= lastYear &
                          timePointYears >= firstDataYear, ]
    setnames(parentData, c(itemVar, elementVar),
             c("measuredItemParentCPC", "measuredElementParent"))
    ## TODO (Michael): This should not be called child data, since there
    ##                 is no child data, it is parent data but with the
    ##                 mapping table and variable names changed.
    childData = merge(parentData, selectedMeat,
                      by = c("measuredItemParentCPC", "measuredElementParent"))
    childData[, c("measuredItemParentCPC", "measuredElementParent") := NULL]
    setnames(childData, c("measuredItemChildCPC", "measuredElementChild"),
             c(itemVar, elementVar))


    ## The (childData) data frame contains the value of the parent
    ## commodity, while the (data) dataframe contains all the data. After
    ## the merge, we over write the values in (data) from the values in
    ## (childData).
    ##
    ## NOTE (Michael): The merge should be set to all.y = TRUE and only
    ##                 restrict to the set that should be over-written.
    dataMerged = merge(dataCopy, childData, all = TRUE, suffixes = c("", ".new"),
                       by = c(areaVar, itemVar, elementVar, yearVar))
    dataMerged[!is.na(Value.new),
               c("Value", "flagObservationStatus", "flagMethod") :=
               list(Value.new, flagObservationStatus.new, flagMethod.new)]
    dataMerged[, c("Value.new", "flagObservationStatus.new",
                   "flagMethod.new") := NULL]
    dataMerged
}
