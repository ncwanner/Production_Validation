##' This function imputes the whole production domain.
##' 
##' The function will impute production, area harvested and yield at the same
##' time.
##' 
##' Transformation in the yield formula is not allowed and will not be taken
##' into account.
##' 
##' @param data The data
##' @param processingParameters A list of the parameters for the production 
##'   processing algorithms.  See defaultProductionParameters() for a starting 
##'   point.
##' @param yieldImputationParameters A list of the parameters for the yield
##'   imputation.  See defaultImputationParameters() for a starting point.
##' @param productionImputationParameters A list of the parameters for the 
##'   production imputation.  See defaultImputationParameters() for a starting 
##'   point.
##' @param unitConversion The multiplicative factor used for the particular
##'   commodity in calculating the yield.
##'   
##' @export
##' 
##' @import faoswsImputation
##' @import data.table
##' 

imputeProductionDomain = function(data, processingParameters,
                                  yieldImputationParameters,
                                  productionImputationParameters,
                                  unitConversion){
    originDataType = sapply(data, FUN = typeof)

    cat("Initializing ... \n")
    dataCopy = copy(data)
    ### Data Quality Checks
    ensureImputationInputs(data = dataCopy,
                           imputationParameters = yieldImputationParameters)
    ensureImputationInputs(data = dataCopy,
                           imputationParameters = productionImputationParameters)
    ensureProductionInputs(data = dataCopy,
                           processingParameters = processingParameters)

    setkeyv(x = dataCopy, cols = c(processingParameters$byKey,
                                   processingParameters$yearValue))
    dataCopy = processProductionDomain(data = dataCopy,
                                       processingParameters = processingParameters)

    
    computeYield(dataCopy, newMethodFlag = "i",
                 processingParameters = processingParameters,
                 unitConversion = unitConversion)
    ## Check whether all values are missing
    ##
    ## NOTE (Michael): The imputation module fails if all yield are
    ##                 missing.
    allYieldMissing = all(is.na(dataCopy[[processingParameters$yieldValue]]))
    
    if(!all(allYieldMissing)){
        ## Step two: Impute Yield
        cat("Imputing Yield ...\n")
        n.missYield = sum(is.na(dataCopy[[processingParameters$yieldValue]]))
        ## if(!missing(yieldFormula))
        ##     yieldFormula =
        ##     as.formula(gsub(yearValue, "yearValue",
        ##                     gsub(yieldValue, "yieldValue",
        ##                          deparse(yieldFormula))))
        
        imputeVariable(data = dataCopy,
                       imputationParameters = yieldImputationParameters)
        n.missYield2 = length(which(is.na(
            dataCopy[[processingParameters$yieldValue]])))
        cat("Number of values imputed: ", n.missYield - n.missYield2, "\n")
        cat("Number of values still missing: ", n.missYield2, "\n")

        ## Balance production now using imputed yield
        balanceProduction(data = dataCopy, processingParameters = processingParameters,
                          unitConversion = unitConversion)

        ## step three: Impute production
        cat("Imputing Production ...\n")
        n.missProduction = length(which(is.na(
            dataCopy[[processingParameters$productionValue]])))

        imputeVariable(data = dataCopy,
                       imputationParameters = productionImputationParameters)

        n.missProduction2 = length(which(is.na(
            dataCopy[[processingParameters$productionValue]])))
        cat("Number of values imputed: ",
            n.missProduction - n.missProduction2, "\n")
        cat("Number of values still missing: ", n.missProduction2, "\n")

        ## step four: balance area harvested
        cat("Imputing Area Harvested ...\n")
        n.missAreaHarvested =
            length(which(is.na(
                dataCopy[[processingParameters$areaHarvestedValue]])))

        balanceAreaHarvested(data = dataCopy,
                             processingParameters = processingParameters,
                             unitConversion = unitConversion)

        n.missAreaHarvested2 =
            length(which(is.na(
                dataCopy[[processingParameters$areaHarvestedValue]])))
        cat("Number of values imputed: ",
            n.missAreaHarvested - n.missAreaHarvested2, "\n")
        cat("Number of values still missing: ", n.missAreaHarvested2, "\n")
    } else {
        warning("The input dataset contains insufficient data for imputation to perform!")
    }
    
    ## This is to ensure the data type of the output is identical to
    ## the input data.
    dataCopy[, `:=`(colnames(dataCopy),
                    lapply(colnames(dataCopy),
                           FUN = function(x){
                               if(x %in% names(originDataType)){
                                   as(.SD[[x]], originDataType[[x]])
                               } else {
                                   .SD[[x]]
                               }
                           }))]
    dataCopy
}
