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
##'     processing algorithms. See defaultProductionParameters() for a starting
##'     point.
##' @param yieldImputationParameters A list of the parameters for the yield
##'     imputation. See defaultImputationParameters() for a starting point.
##' @param productionImputationParameters A list of the parameters for the
##'     production imputation. See defaultImputationParameters() for a starting
##'     point.
##' @param areaHarvestedImputationParameters A list of the parameters for the
##'     area harvested imputation. See defaultImputationParameters() for a
##'     starting point.
##'
##' @export
##'
##' @import faoswsImputation
##' @import data.table
##'

imputeProductionDomain = function(data,
                                  processingParameters,
                                  areaHarvestedImputationParameters,
                                  yieldImputationParameters,
                                  productionImputationParameters){
    originDataType = sapply(data, FUN = typeof)

    cat("Initializing ... \n")
    dataCopy = copy(data)
    ## Data Quality Checks
    ensureImputationInputs(data = dataCopy,
                           imputationParameters = yieldImputationParameters)
    ensureImputationInputs(data = dataCopy,
                           imputationParameters = productionImputationParameters)

    ensureProductionInputs(dataCopy,
                           processingParameters = processingParameters,
                           returnData = FALSE,
                           normalised = FALSE)

    setkeyv(x = dataCopy, cols = c(processingParameters$areaVar,
                                   processingParameters$yearValue))
    dataCopy = processProductionDomain(data = dataCopy,
                                       processingParameters = processingParameters)


    computeYield(dataCopy, processingParameters = processingParameters)
    ## Check whether all values are missing
    ##
    ## NOTE (Michael): The imputation module fails if all yield are
    ##                 missing.
    allYieldMissing = all(is.na(dataCopy[[processingParameters$yieldValue]]))
    allProductionMissing = all(is.na(dataCopy[[processingParameters$productionValue]]))
    allAreaMissing = all(is.na(dataCopy[[processingParameters$areaHarvestedValue]]))


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
        ## TODO (Michael): Remove imputed zero yield as yield can not be zero by
        ##                 definition. This probably should be handled in the
        ##                 imputation parameter.
        dataCopy =
            removeZeroYield(dataCopy,
                            yieldValue = processingParameters$yieldValue,
                            yieldObsFlag = processingParameters$yieldObservationFlag,
                            yieldMethodFlag = processingParameters$yieldMethodFlag)
        n.missYield2 = length(which(is.na(
            dataCopy[[processingParameters$yieldValue]])))
        cat("Number of values imputed: ", n.missYield - n.missYield2, "\n")
        cat("Number of values still missing: ", n.missYield2, "\n")

        ## Balance production now using imputed yield
        balanceProduction(data = dataCopy,
                          processingParameters = processingParameters)

        ## NOTE (Michael): Check again whether production is available
        ##                 now after it is balanced.
        allProductionMissing = all(is.na(dataCopy[[processingParameters$productionValue]]))
    } else {
        warning("The input dataset contains insufficient data for imputation to perform!")
    }

    if(!all(allProductionMissing)){
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
    } else {
        warning("The input dataset contains insufficient data for imputation to perform!")
    }

    ## step four: balance area harvested
    cat("Imputing Area Harvested ...\n")
    n.missAreaHarvested =
        length(which(is.na(
            dataCopy[[processingParameters$areaHarvestedValue]])))

    balanceAreaHarvested(data = dataCopy,
                         processingParameters = processingParameters)
    allAreaMissing = all(is.na(dataCopy[[processingParameters$areaHarvestedValue]]))

    if(!all(allAreaMissing)){
        ## HACK (Michael): This is to ensure the area harvested are also
        ##                 imputed. Then we delete all computed yield and
        ##                 then balance again. This causes the yield not
        ##                 comforming to the imputation model.
        ##
        ##                 This whole function should be re-writtened so
        ##                 that an algorithm similar to the EM algorithm
        ##                 estimates and impute the triplet in a conherent
        ##                 way.
        ##
        ##                 Issue #88
        imputeVariable(data = dataCopy,
                       imputationParameters = areaHarvestedImputationParameters)
        dataCopy[!is.na(get(processingParameters$areaHarvestedValue)) &
                 !is.na(get(processingParameters$productionValue)) &
                 !(get(processingParameters$yieldObservationFlag) %in% c("", "*")),
                 `:=`(c(processingParameters$yieldValue,
                        processingParameters$yieldObservationFlag,
                        processingParameters$yieldMethodFlag),
                      list(NA, "M", "u"))]
        computeYield(dataCopy,
                     processingParameters = processingParameters)
        imputeVariable(data = dataCopy,
                       imputationParameters = yieldImputationParameters)
    } ## End of HACK.
    n.missAreaHarvested2 =
        length(which(is.na(
            dataCopy[[processingParameters$areaHarvestedValue]])))
    cat("Number of values imputed: ",
        n.missAreaHarvested - n.missAreaHarvested2, "\n")
    cat("Number of values still missing: ", n.missAreaHarvested2, "\n")


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
