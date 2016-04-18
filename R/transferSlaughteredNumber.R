##' This function is essentially the reverse of the
##' \code{transferAnimalNumber} where the animal slaughtered element
##' of the child commodity (e.g. cattle meat) is transfered to the
##' animal number element of the parent commodity (e.g. cattle).
##'
##' @param preUpdatedData This is the data to be updated
##' @param imputationResult This is the result of the imputation, the
##'     imputed value contains updated slaughtered number to be
##'     updated in the animal numbers
##'
##' @return A data.table object which contains the updated animal
##'     number.
##'
##' @export
##'

transferSlaughteredNumber = function(preUpdatedData, imputationResult){
    childData = copy(imputationResult)
    setnames(childData, c(itemVar, elementVar),
             c("measuredItemChildCPC", "measuredElementChild"))
    parentData = merge(childData, toProcess,
                       by = c("measuredItemChildCPC", "measuredElementChild"))
    parentData[, c("measuredItemChildCPC", "measuredElementChild") := NULL]
    setnames(parentData, c("measuredItemParentCPC", "measuredElementParent"),
             c(itemVar, elementVar))
    parentData[, timePointYears := as.character(timePointYears)]

    ## Only need to keep the updated data
    updatedData = merge(preUpdatedData, parentData, all.y = TRUE,
                        suffixes = c("", ".new"),
                        by = c(areaVar, itemVar, elementVar, yearVar))
    updatedData = updatedData[is.na(Value) & !is.na(Value.new), ]
    updatedData[, c("Value", "flagObservationStatus", "flagMethod") :=
                list(Value.new, flagObservationStatus.new, flagMethod.new)]
    updatedData[, c("Value.new", "flagObservationStatus.new",
                    "flagMethod.new") := NULL]
    updatedData
}
