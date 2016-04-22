imputeMeatTriplet = function(meatKey, minObsForEst = 5){
    cat("Reading in the data...\n")
    datasets = getProductionData(meatKey)
    ## Ignore indigenous/biological:
    datasets$formulaTuples = datasets$formulaTuples[nchar(input) == 4, ]

    
    
    for(i in 1:nrow(datasets$formulaTuples)){
        ## For convenience, let's save the formula tuple to "formula"
    
        message("Processing pair ", i, " of ", nrow(datasets$formulaTuples),
                " element triples.")
        cleanedData = cleanData(datasets, i = i, maxYear = 2014)


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

        
        removedSingleEntryData =
            removeSingleEntryCountry(currentData,
                                     params = processingParams)

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

        
        ## For the actual imputation, we must pass all the data (as the 
        ## global models should use all the information available). 
        ## However, we'll have to delete some of the imputations
        ## (corresponding to series without enough official data) and then
        ## rerun the imputation.
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
        finalData = filterTimeRange(data = removedZeroData,
                                    firstYear = firstYear,
                                    lastYear = lastYear)            
    } ## close item type for loop
    finalData
} ## close try block
