##' This is function performs imputation on the triplet element. This is an
##' implementation from Josh to account to exclude estimates when sufficient
##' data is available or use estimates when they are not.
##'
##' NOTE (Michael): This function should ultimately be removed, it contains a
##'                 lot of hacks and manual estimated values with method flag
##'                 'f' should not be imputed.
##'
##' @param meatKey The Datakey object
##' @param minObsForEst The minimum number of observation required to keep
##'     'E'stimated values.
##' @return Imputed time series
##' @export

imputeWithAndWithoutEstimates = function(meatKey, minObsForEst = 5){
    cat("Reading in the data...\n")
    datasets = getProductionData(meatKey)

    ## NOTE (Michael): Sometimes there are no data in the database,
    ##                 this case, we simply return an empty data table.

    if(NROW(datasets$query) > 0){
        datasets$query = denormalise(preProcessing(normalise(datasets$query)),
                                     denormaliseKey = "measuredElement")
        datasets$formulaTuples =
            removeIndigenousBiologicalMeat(datasets$formulaTuples)
        ## Create a placeholder to merge, this also ensures the
        ## imputation is complete.
        finalData =
            datasets$query[, .(geographicAreaM49, measuredItemCPC, timePointYears)]
        setkeyv(finalData,
                c("geographicAreaM49", "measuredItemCPC", "timePointYears"))
        for(i in 1:nrow(datasets$formulaTuples)){
            ## For convenience, let's save the formula tuple to "formula"
            message("Processing pair ", i, " of ", nrow(datasets$formulaTuples),
                    " element triples.")

            ## Split the data by formula for easy processing
            currentFormula = datasets$formulaTuples[i, ]
            selectedElements =
                currentFormula[, unlist(.(input, productivity, output))]
            currentData =
                datasets$query[, c(key(finalData),
                                   grep(paste0(paste0(selectedElements, "$"),
                                               collapse = "|"),
                                        colnames(datasets$query),
                                        value = TRUE)),
                               with = FALSE]

            currentPrefix = datasets$prefixTuples
            ## Setup for the imputation
            processingParams =
                productionProcessingParameters(
                    GetDatasetConfig("agriculture", "aproduction"),
                    productionCode = currentFormula[, output],
                    yieldCode = currentFormula[, productivity],
                    areaHarvestedCode = currentFormula[, input]
                )

            p = getImputationParameters(datasets, i = i)
            ## NOTE (Michael): Stop the plotting.
            p$yieldParams$plotImputation = ""
            p$productionParams$plotImputation = ""
            p$areaHarvestedParams$plotImputation = ""
            yieldParams = p$yieldParams
            productionParams = p$productionParams
            areaHarvestedParams = p$areaHarvestedParams
            processedData =
                processProductionDomain(data = currentData,
                                        processingParameters = processingParams)


            ## HACK (Michael): The following is to account for the case
            ##                 where the data becomes empty after the
            ##                 processing.
            if(NROW(processedData) < 1)
                next

            ## Imputation is a bit tricky, as we want to exclude previously
            ## estimated data if we have "enough" official/semi-official data in
            ## a time series but we want to use estimates if we don't have
            ## enough official/semi-official data.  "Enough" is specified by
            ## minObsForEst.
            flags = paste0(currentPrefix$flagObsPrefix,
                           currentFormula[, c("input", "productivity", "output"),
                                          with = FALSE])

            validObsCnt =
                useEstimateForTimeSeriesImputation(data = processedData,
                                                   areaObsFlagVar = flags[1],
                                                   yieldObsFlagVar = flags[2],
                                                   prodObsFlagVar = flags[3],
                                                   minObsForEst = minObsForEst)

            ## For the actual imputation, we must pass all the data
            ## (as the global models should use all the information
            ## available).  However, we'll have to delete some of the
            ## imputations (corresponding to series without enough
            ## official data) and then rerun the imputation.
            origData = copy(processedData)
            processingParams$removeManualEstimation = TRUE
            cat("Imputation without Manual Estimates\n")
            imputation1 =
                imputeProductionDomain(copy(origData),
                                       processingParameters = processingParams,
                                       areaHarvestedImputationParameters =
                                           areaHarvestedParams,
                                       yieldImputationParameters = yieldParams,
                                       productionImputationParameters =
                                           productionParams,
                                       unitConversion =
                                           currentFormula[, unitConversion])
            ## Now impute while leaving estimates in
            processingParams$removeManualEstimation = FALSE
            simplerModels = allDefaultModels()
            simplerModels = simplerModels[c("defaultMean", "defaultLm",
                                            "defaultExp", "defaultNaive",
                                            "defaultMixedModel")]
            yieldParams$ensembleModels = simplerModels
            productionParams$ensembleModels = simplerModels
            areaHarvestedParams$ensembleModels = simplerModels
            cat("Imputation with Manual Estimates\n")
            imputation2 =
                imputeProductionDomain(copy(origData),
                                       processingParameters = processingParams,
                                       areaHarvestedImputationParameters =
                                           areaHarvestedParams,
                                       yieldImputationParameters = yieldParams,
                                       productionImputationParameters =
                                           productionParams,
                                       unitConversion = currentFormula[, unitConversion])

            ## Formula to only include series with enough data:
            imputationNormalised1 = normalise(imputation1)
            valuesImputedWithoutEstimates =
                selectUseEstimate(data = imputationNormalised1,
                                  useEstimatesTable = validObsCnt,
                                  useEstimates = FALSE)

            ## Now add in the values imputed in the second round
            imputationNormalised2 = normalise(imputation2)
            valuesImputedWithEstimates =
                selectUseEstimate(data = imputationNormalised2,
                                  useEstimatesTable = validObsCnt,
                                  useEstimates = TRUE)

            ## Bring together the estimates and reshape them:
            valuesImputed = combineImputation(valuesImputedWithoutEstimates,
                                              valuesImputedWithEstimates)


            ## The hack of switching between imputation with/without manual 'E'
            ## stimates will eventually be removed.
            imputation =
                denormalise(valuesImputed, denormaliseKey = "measuredElement")

            setkeyv(imputation,
                    cols = c("geographicAreaM49", "measuredItemCPC",
                             "timePointYears"))

            finalData = merge(finalData, imputation, by = key(finalData),
                              all.x = TRUE)
        } ## close item type for loop
    } else {
        finalData = datasets$query
    }
    finalData
} ## close try block
