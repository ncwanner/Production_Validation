##' This function filters the selected keys to the selected meats
##'
##' @param key The DatasetKey object of the current session
##' @param selectedMeatTable The table returned by the function
##'     \code{getAnimalMeatMapping}.
##'
##' @return A character list containing the required meat commodity code.
##' @export

getSessionMeatSelection = function(key, selectedMeatTable){

    sessionCommodity = key@dimensions$measuredItemCPC@keys
    ## Just impute the meat elements
    sessionMeat =
        sessionCommodity[sessionCommodity %in%
                         selectedMeatTable$measuredItemChildCPC]
    sessionMeat = as.character(sessionMeat)
    sessionMeat
}
