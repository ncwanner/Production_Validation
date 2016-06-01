##' This is function performs imputation on the triplet element. This is an
##' implementation from Josh to account to exclude estimates when sufficient
##' data is available or use estimates when they are not.
##'
##' NOTE (Michael): This switching between imputation with/without manual 'E'
##'                 stimates will eventually be removed.
##'
##' @param data The data to be imputed
##' @param processingParameters A list of the parameters for the production
##'     processing algorithms. See \code{productionProcessingParameters} for a
##'     starting point.
##' @param formulaParameters A list holding the names and parmater of formulas.
##'     See \code{productionFormulaParameters}.
##' @param imputationParameters A list holding the imputation parameters, see
##'     \code{getImputationParameters}.
##' @param minObsForEst The minimum number of observation required to keep
##'     'E'stimated values.
##' @return Imputed time series
##' @export

imputeWithAndWithoutEstimates = function(data,
                                         processingParameters,
                                         formulaParameters,
                                         imputationParameters,
                                         minObsForEst = 5){
    ## NOTE (Michael): Sometimes there are no data in the database,
    ##                 this case, we simply return an empty data table.
    dataCopy = copy(data)
    if(NROW(dataCopy) > 0){

        ## NOTE (Michael): Stop the plotting.
        imputationParameters$yieldParams$plotImputation = ""
        imputationParameters$productionParams$plotImputation = ""
        imputationParameters$areaHarvestedParams$plotImputation = ""

        yieldParams = imputationParameters$yieldParams
        productionParams = imputationParameters$productionParams
        areaHarvestedParams = imputationParameters$areaHarvestedParams

        ## HACK (Michael): The following is to account for the case
        ##                 where the data becomes empty after the
        ##                 processing.
        if(NROW(dataCopy) < 1)
            next

        ## Imputation is a bit tricky, as we want to exclude previously
        ## estimated data if we have "enough" official/semi-official data in
        ## a time series but we want to use estimates if we don't have
        ## enough official/semi-official data.  "Enough" is specified by
        ## minObsForEst.

        validObsCnt =
            useEstimateForTimeSeriesImputation(
                data = dataCopy,
                areaObsFlagVar =
                    formulaParameters$areaHarvestedObservationFlag,
                yieldObsFlagVar =
                    formulaParameters$yieldObservationFlag,
                prodObsFlagVar =
                    formulaParameters$productionObservationFlag,
                minObsForEst = minObsForEst)

        ## For the actual imputation, we must pass all the data
        ## (as the global models should use all the information
        ## available).  However, we'll have to delete some of the
        ## imputations (corresponding to series without enough
        ## official data) and then rerun the imputation.
        processingParameters$removeManualEstimation = TRUE
        message("Imputation without Manual Estimates\n")
        imputation1 =
            imputeProductionTriplet(copy(dataCopy),
                                   processingParameters = processingParameters,
                                   formulaParameters = formulaParameters,
                                   areaHarvestedImputationParameters =
                                       areaHarvestedParams,
                                   yieldImputationParameters = yieldParams,
                                   productionImputationParameters =
                                       productionParams)

        ## Now impute while leaving estimates in
        processingParameters$removeManualEstimation = FALSE
        simplerModels = allDefaultModels()
        simplerModels = simplerModels[c("defaultMean", "defaultLm",
                                        "defaultExp", "defaultNaive",
                                        "defaultMixedModel")]
        yieldParams$ensembleModels = simplerModels
        productionParams$ensembleModels = simplerModels
        areaHarvestedParams$ensembleModels = simplerModels
        message("Imputation with Manual Estimates\n")
        imputation2 =
            imputeProductionTriplet(copy(dataCopy),
                                   processingParameters = processingParameters,
                                   formulaParameters = formulaParameters,
                                   areaHarvestedImputationParameters =
                                       areaHarvestedParams,
                                   yieldImputationParameters = yieldParams,
                                   productionImputationParameters =
                                       productionParams)

        ## Select imputed data without estimates
        imputationNormalised1 = normalise(imputation1)
        valuesImputedWithoutEstimates =
            selectUseEstimate(data = imputationNormalised1,
                              useEstimatesTable = validObsCnt,
                              useEstimates = FALSE)

        ## Select imputed data with estimates
        imputationNormalised2 = normalise(imputation2)
        valuesImputedWithEstimates =
            selectUseEstimate(data = imputationNormalised2,
                              useEstimatesTable = validObsCnt,
                              useEstimates = TRUE)

        ## Bring together the estimates and reshape them:
        valuesImputed = combineImputation(valuesImputedWithoutEstimates,
                                          valuesImputedWithEstimates)



        finalData =
            denormalise(valuesImputed, denormaliseKey = "measuredElement")

    } else {
        finalData = datasets$query
    }
    finalData
}
