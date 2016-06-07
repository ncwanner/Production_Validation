##' Function to transfer values from parent to child.
##'
##' When values transferred between parent and child, shares are applied. To
##' transfer parent to children, we multiply by the share in order to obtain the
##' share of parents used to produce the child commodity. In the reversed
##' direction, the values are divided by the shares.
##'
##' @param parentData The animal slaughtered data from animal commodity.
##' @param childData The animal slaughtered data from meat commodity.
##' @param mappingTable The mapping table between the parent and the child.
##' @param parentToChild logical, if true, slaughtered animal are transferred
##'     from animal commodity to meat, otherwise the otherway around.
##'
##' @return The transferred data
##' @export

transferParentToChild = function(parentData,
                                 childData,
                                 mappingTable,
                                 parentToChild = TRUE){

    ## Input check
    ##
    ## TODO (Michael): Need an input check for mapping table as well.
    requiredColumn = c("geographicAreaM49",
                       "measuredItemCPC",
                       "measuredElement",
                       "timePointYears",
                       "Value",
                       "flagObservationStatus",
                       "flagMethod")

    ensureDataInput(data = childData,
                    requiredColumn = requiredColumn,
                    returnData = FALSE)
    ensureDataInput(data = parentData,
                    requiredColumn = requiredColumn,
                    returnData = FALSE)


    ## Convert the names of the table
    childDataCopy = copy(childData)
    parentDataCopy = copy(parentData)

    setnames(x = childDataCopy,
             old = c("measuredItemCPC", "measuredElement",
                     "Value", "flagObservationStatus", "flagMethod"),
             new = c("measuredItemChildCPC", "measuredElementChild",
                     paste0(c("Value", "flagObservationStatus", "flagMethod"),
                            "_child")))
    setnames(x = parentDataCopy,
             old = c("measuredItemCPC", "measuredElement",
                     "Value", "flagObservationStatus", "flagMethod"),
             new = c("measuredItemParentCPC", "measuredElementParent",
                     paste0(c("Value", "flagObservationStatus", "flagMethod"),
                            "_parent")))

    ## Merge the three input dataset
    childMergeCol = intersect(colnames(childDataCopy),
                              colnames(mappingTable))
    childDataMapped = merge(childDataCopy, mappingTable,
                            by = childMergeCol, all.y = TRUE)

    parentMergeCol = intersect(colnames(parentDataCopy),
                               colnames(mappingTable))
    parentDataMapped = merge(parentDataCopy, mappingTable,
                             by = parentMergeCol, all.y = TRUE)

    mergeAllCol = intersect(colnames(childDataMapped),
                            colnames(parentDataMapped))
    parentChildMergedData = merge(childDataMapped, parentDataMapped,
                                  by = mergeAllCol, all = TRUE)

    ## If share is missing, it is defaulted to 1
    parentChildMergedData[is.na(share), `:=`(c("share"), 1)]

    ## Transfer the value from parent to child or the other way round.
    ##
    ## TODO (Michael): Need to check what can be copied.
    ##
    ## NOTE (Michael): I think everything should be copied except for protected
    ##                 data. An error should be thrown when both value are
    ##                 protected data.
    if(parentToChild){
        parentChildMergedData[, `:=`(c("measuredItemCPC",
                                       "measuredElement",
                                       "Value",
                                       "flagObservationStatus",
                                       "flagMethod"),
                                     list(measuredItemChildCPC,
                                          measuredElementChild,
                                          Value_parent * share,
                                          flagObservationStatus_parent,
                                          "i"))]
    } else {
        ## TODO (Michael): If share is zero, then the value of the child should
        ##                 be zero as well. An error should be thrown here.
        parentChildMergedData[share != 0,
                              `:=`(c("measuredItemCPC",
                                     "measuredElement",
                                     "Value",
                                     "flagObservationStatus",
                                     "flagMethod"),
                                   list(measuredItemParentCPC,
                                        measuredElementParent,
                                        Value_child/share,
                                        flagObservationStatus_child,
                                        "i"))]
    }


    dataToBeReturned =
        subset(parentChildMergedData,
               select = requiredColumn,
               subset = !is.na(Value))


    dataToBeReturned
}
