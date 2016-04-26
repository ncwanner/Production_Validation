##' This is a wrapper for all the data manipulation steps before the preparation
##' of the imputation.
##' 
##' @param data The data
##' @param processingParameters A list of the parameters for the production 
##'   processing algorithms.  See defaultProductionParameters() for a starting 
##'   point.
##'   
##' @export
##' 
##' @return Currently it returns the passed data.table after performing some
##'   checks and clean up of the data.  Eventually, it should modify the
##'   data.table in place, but this will require an update to data.table (see
##'   comment by the return statement).
##'   

processProductionDomain = function(data, processingParameters){
    
    ### Data Quality Checks
    if(!exists("ensuredProductionData") || !ensuredProductionData)
        ensureProductionInputs(data = data,
                               processingParameters = processingParameters)
    
    ### processingParameters will be referenced alot, so rename to p
    p = processingParameters
    
    ### Remove prior imputations
    if(processingParameters$removePriorImputation){
        faoswsUtil::removeImputation(data = data,
                    value = p$areaHarvestedValue,
                    observationFlag = p$areaHarvestedObservationFlag,
                    methodFlag = p$areaHarvestedMethodFlag,
                    missingObservationFlag = p$naFlag,
                    imputedFlag = p$imputedFlag)
        faoswsUtil::removeImputation(data = data,
                    value = p$yieldValue,
                    observationFlag = p$yieldObservationFlag,
                    methodFlag = p$yieldMethodFlag,
                    missingObservationFlag = p$naFlag,
                    imputedFlag = p$imputedFlag)
        faoswsUtil::removeImputation(data = data,
                    value = p$productionValue,
                    observationFlag = p$productionObservationFlag,
                    methodFlag = p$productionMethodFlag,
                    missingObservationFlag = p$naFlag,
                    imputedFlag = p$imputedFlag)
    }

    emptyEntry = (is.na(data[[p$productionObservationFlag]]) |
                  is.na(data[[p$yieldObservationFlag]])|
                  is.na(data[[p$areaHarvestedObservationFlag]]))

    data = data[!emptyEntry, ]

    
    ## HACK (Michael): Imputed flag should always be removed, change
    ##                 the above condition to remove manual
    ##                 estimates. If imputedFlag "I" is not removed,
    ##                 then they are always retained in the database.
    faoswsUtil::removeImputation(data = data,
                                 value = p$areaHarvestedValue,
                                 observationFlag = p$areaHarvestedObservationFlag,
                                 methodFlag = p$areaHarvestedMethodFlag,
                                 missingObservationFlag = p$naFlag,
                                 imputedFlag = "I")
    faoswsUtil::removeImputation(data = data,
                                 value = p$yieldValue,
                                 observationFlag = p$yieldObservationFlag,
                                 methodFlag = p$yieldMethodFlag,
                                 missingObservationFlag = p$naFlag,
                                 imputedFlag = "I")
    faoswsUtil::removeImputation(data = data,
                                 value = p$productionValue,
                                 observationFlag = p$productionObservationFlag,
                                 methodFlag = p$productionMethodFlag,
                                 missingObservationFlag = p$naFlag,
                                 imputedFlag = "I")
    ### Assign NA's when the flag is missing
    faoswsUtil::remove0M(data = data,
             value = p$areaHarvestedValue,
             flag = p$areaHarvestedObservationFlag,
             naFlag = p$naFlag)
    faoswsUtil::remove0M(data = data,
             value = p$yieldValue,
             flag = p$yieldObservationFlag,
             naFlag = p$naFlag)
    faoswsUtil::remove0M(data = data,
             value = p$productionValue,
             flag = p$productionObservationFlag,
             naFlag = p$naFlag)
    
    ### Remove conflicting/illogical zeros
    if(p$removeConflictValues){
        faoswsUtil::removeZeroConflict(data = data,
                           value1 = p$areaHarvestedValue,
                           value2 = p$productionValue,
                           observationFlag1 = p$areaHarvestedObservationFlag,
                           observationFlag2 = p$productionObservationFlag,
                           methodFlag1 = p$areaHarvestedMethodFlag,
                           methodFlag2 = p$productionMethodFlag,
                           missingObservationFlag = p$naFlag)
    }

    ### Remove byKey groups that have no data
    ## faoswsUtil::removeNoInfo(data = data,
    ##              value = p$yieldValue,
    ##              observationFlag = p$yieldObservationFlag,
    ##              byKey = p$byKey)
    ## removeNoInfo assigns the new data.table to the variable "data" in the
    ## environment of this function.  Thus, to ensure "data" is returned to the
    ## caller of this function, assign the data.table to the calling environment.
    ## This should be removed/fixed once row deletion by reference is
    ## implemented for data.table, see
    ## http://stackoverflow.com/questions/10790204/how-to-delete-a-row-by-reference-in-r-data-table
    ## This return approach seems a bit too hacky.  Let's just return it the normal way.
    ## dataTableName = as.character(match.call()$data)
    ## assign(x = dataTableName, value = data, envir = parent.frame(1))
    return(data)
}
