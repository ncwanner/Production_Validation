imputeMeatTriplet = function(meatKey, minObsForEst = 5){
    cat("Reading in the data...\n")
    datasets = getProductionData(meatKey)
    ## Ignore indigenous/biological:
    datasets$formulaTuples = datasets$formulaTuples[nchar(input) == 4, ]

    ## NOTE (Michael): Sometimes there are no data in the database,
    ##                 this case, we simply return an empty data table.
    if(NROW(datasets$query) > 0){

        ## Create a placeholder
        finalData = datasets$query[0, ]
        setkeyv(finalData, c("geographicAreaM49", "measuredItemCPC", "timePointYears"))
        for(i in 1:nrow(datasets$formulaTuples)){
            ## For convenience, let's save the formula tuple to "formula"
            message("Processing pair ", i, " of ", nrow(datasets$formulaTuples),
                    " element triples.")
            cleanedData = cleanData(datasets, i = i, maxYear = 2014)
            warning("The hard coded year in this function should be removed!!!")


            ## Split the data for easy reference
            currentData = cleanedData$query
            currentFormula = cleanedData$formulaTuples[i, ]
            currentPrefix = cleanedData$prefixTuples

            ## Setup for the imputation
            processingParams =
                defaultProcessingParameters(productionValue = currentFormula[, output],
                                            yieldValue = currentFormula[, productivity],
                                            areaHarvestedValue = currentFormula[, input])
            processingParams$imputedFlag = c("I", "E")
            p = getImputationParameters(cleanedData, i = i)
            ## NOTE (Michael): Stop the plotting.
            p$yieldParams$plotImputation = ""
            p$productionParams$plotImputation = ""
            yieldParams = p$yieldParams
            productionParams = p$productionParams

            processedData =
                processProductionDomain(data = currentData,
                                        processingParameters = processingParams)

            removedSingleEntryData =
                removeSingleEntryCountry(processedData,
                                         params = processingParams)

            ## HACK (Michael): The following is to account for the case
            ##                 where the data becomes empty after the
            ##                 processing.
            if(NROW(removedSingleEntryData) < 1)
                next

            forcedZero =
                getForcedZeroKey(removedSingleEntryData,
                                 processingParam = processingParams,
                                 productionParams = productionParams)

            ## Imputation is a bit tricky, as we want to exclude previously 
            ## estimated data if we have "enough" official/semi-official data in
            ## a time series but we want to use estimates if we don't have 
            ## enough official/semi-official data.  "Enough" is specified by
            ## minObsForEst.
            flags = paste0(currentPrefix$flagObsPrefix,
                           currentFormula[, c("productivity", "output"), with = FALSE])

            validObsCnt =
                useEstimateForTimeSeriesImputation(data = removedSingleEntryData,
                                                   yieldObsFlagVar = flags[1],
                                                   prodObsFlagVar = flags[2],
                                                   minObsForEst = minObsForEst)

            ## For the actual imputation, we must pass all the data
            ## (as the global models should use all the information
            ## available).  However, we'll have to delete some of the
            ## imputations (corresponding to series without enough
            ## official data) and then rerun the imputation.
            origData = copy(removedSingleEntryData)
            processingParams$removePriorImputation = TRUE
            cat("Imputation without Manual Estimates\n")
            imputation1 =
                imputeProductionDomain(copy(origData),
                                       processingParameters = processingParams,
                                       yieldImputationParameters = yieldParams,
                                       productionImputationParameters =
                                           productionParams,
                                       unitConversion = currentFormula[, unitConversion])
            ## Now impute while leaving estimates in
            processingParams$removePriorImputation = FALSE
            simplerModels = allDefaultModels()
            simplerModels = simplerModels[c("defaultMean", "defaultLm",
                                            "defaultExp", "defaultNaive",
                                            "defaultMixedModel")]
            yieldParams$ensembleModels = simplerModels
            productionParams$ensembleModels = simplerModels
            cat("Imputation with Manual Estimates\n")
            imputation2 =
                imputeProductionDomain(copy(origData),
                                       processingParameters = processingParams,
                                       yieldImputationParameters = yieldParams,
                                       productionImputationParameters =
                                           productionParams,
                                       unitConversion = currentFormula[, unitConversion])
            ## Take all the I/e values that have just been estimated, but don't
            ## include I/i (as balanced observations may not be correct, because
            ## we could use estimates for yield imputation and not for
            ## production, for example).
            valuesImputed1 =
                getUpdatedObs(dataOld = origData,
                              dataNew = imputation1,
                              key = c("geographicAreaM49", "timePointYears"),
                              wideVarName = "measuredElement")
            ## Formula to only include series with enough data:
            valuesImputedWithoutEstimates =
                getImputedValues(data = valuesImputed1,
                                 useEstimatesTable = validObsCnt,
                                 useEstimates = FALSE)

            ## Now add in the values imputed in the second round
            valuesImputed2 =
                getUpdatedObs(dataOld = origData,
                              dataNew = imputation2,
                              key = c("geographicAreaM49", "timePointYears"),
                              wideVarName = "measuredElement")
            valuesImputedWithEstimates =
                getImputedValues(data = valuesImputed2,
                                 useEstimatesTable = validObsCnt,
                                 useEstimates = TRUE)

            ## Bring together the estimates and reshape them:
            valuesImputed = combineImputation(valuesImputedWithoutEstimates,
                                              valuesImputedWithEstimates)
            updatedData =
                updateOriginalDataWithImputation(originalData = origData,
                                                 imputedData = valuesImputed,
                                                 yieldElementCode =
                                                     currentFormula[, productivity],
                                                 prodElementCode =
                                                     currentFormula[, output])
            ## Now, use the identity Yield = Production / Area to add in missing
            ## values.
            computeYield(data = updatedData,
                         processingParameters = processingParams,
                         unitConversion = currentFormula[, unitConversion])
            balanceProduction(data = updatedData,
                              processingParameters = processingParams,
                              unitConversion = currentFormula[, unitConversion])
            balanceAreaHarvested(data = updatedData,
                                 processingParameters = processingParams,
                                 unitConversion = currentFormula[, unitConversion])

            ## Remove the observations we don't want to impute on
            ## Use keys so we can do an anti-join
            removedZeroData = removeForcedZero(data = updatedData,
                                               forcedZeroKey = forcedZero)
            imputedData = filterTimeRange(data = removedZeroData,
                                          firstYear = firstYear,
                                          lastYear = lastYear)
            setkeyv(imputedData,
                    cols = c("geographicAreaM49", "measuredItemCPC", "timePointYears"))
            finalData = merge(finalData, imputedData, by = key(finalData))
        } ## close item type for loop
    } else {
        finalData = datasets$query
    }
    finalData
} ## close try block
