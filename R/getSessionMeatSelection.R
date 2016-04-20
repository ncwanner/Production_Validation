getSessionMeatSelection = function(key, selectedMeatTable){

    sessionCommodity = key@dimensions$measuredItemCPC@keys
    ## Just impute the meat elements
    sessionMeat =
        sessionCommodity[sessionCommodity %in%
                         selectedMeatTable$measuredItemChildCPC]
    sessionMeat = as.character(sessionMeat)
    sessionMeat
}
