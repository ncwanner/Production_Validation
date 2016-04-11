## load the library
library(faosws)
library(faoswsUtil)
library(faoswsFlag)
library(faoswsImputation)
library(data.table)
library(splines)
library(lme4)

## Setting up variables
areaVar = "geographicAreaM49"
yearVar = "timePointYears"
itemVar = "measuredItemCPC"
elementVar = "measuredElement"
runParallel = FALSE
yearsModeled = 20

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
DEBUG_MODE = Sys.getenv("R_DEBUG_MODE")

if(!exists("DEBUG_MODE") || DEBUG_MODE == ""){
    cat("Not on server, so setting up environment...\n")
    
    server = "Prod"
    # server = "Prod"
    
    stopifnot(server %in% c("QA", "Prod"))
    ## Define directories
    if(Sys.info()[7] == "josh"){
        apiDirectory = "~/Documents/Github/faoswsProduction/R/"
        R_SWS_SHARE_PATH = ifelse(server == "Prod", "/media/hqlprsws2_prod/",
                                  "/media/hqlprsws1_qa/")
        # runParallel = TRUE
        ## Get SWS Parameters
        SetClientFiles(dir = ifelse(server == "Prod",
                                    "~/R certificate files/Production/",
                                    "~/R certificate files/QA/"))
    } else if(Sys.info()[7] == "rockc_000"){
        apiDirectory = "~/Github/faoswsProduction/R/"
        stop("Can't connect to share drives!")
    } else if(Sys.info()[7] == "mk"){
        apiDirectory = "R/"
        R_SWS_SHARE_PATH = ifelse(server == "Prod",
                                  "/media/sws_prod_shared_drive/",
                                  "/media/sws_qa_shared_drive/")
        SetClientFiles(dir = ifelse(server == "Prod", "~/.R/prod/", "~/.R/qa/"))
    }


    if(server == "Prod"){
        GetTestEnvironment(
            baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
            token = "ad797167-20aa-4ff4-b679-461c96e0da79"
        )
    } else {
        GetTestEnvironment(
            baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
            token = "5eed31f5-2318-470e-a884-c30c3db6d3db"
        )
    }

    ## Source local scripts for this local test
    for(file in dir(apiDirectory, full.names = T))
        source(file)

    suppressPackageStartupMessages(library(doParallel))
    library(foreach)
    registerDoParallel(cores=detectCores(all.tests=TRUE))
}

checkTimeSeriesImputed = function(data, key, valueColumn){
    ## The number of missing values should be either zero or all
    ## missing.
    check = data[, sum(is.na(.SD[[valueColumn]])) == 0 |
                   sum(is.na(.SD[[valueColumn]])) == .N,
                 by = c(key)]
    unimputedTimeSeries = which(!check$V1)
    if(length(unimputedTimeSeries) > 0){
        ## unimputedData = merge(check[unimputedTimeSeries, ],
        ##                       data, by = key, all.x = TRUE)
        stop("Not all time series are imputed")
    }
}

saveDir = paste0(R_SWS_SHARE_PATH, "/browningj/production/")

lastYear = as.numeric(swsContext.computationParams$lastYear)
firstYear = lastYear - yearsModeled + 1 # Rolling time range of yearsModeled years
years = firstYear:lastYear

fullKey = getMainKey(years = years)

subKey = fullKey
uniqueItem = fullKey@dimensions$measuredItemCPC@keys
uniqueItem = data.table(measuredItemCPC = uniqueItem)
uniqueItem[, isPrimary := grepl("^0", measuredItemCPC)]
warning("Primary items should be identified with a table in the SWS!!!")

# completed = dir(saveDir, pattern = "^prodModel.*\\.RData$")
# completed = gsub("prodModel_|_[1-9]\\.RData", "", completed)
completed = NULL
uniqueItem = uniqueItem[!measuredItemCPC %in% completed, ]

##' Run Model
##' 
##' This function provides an additional wrapper that does most of the data 
##' pre-processing prior to the imputation.
##' 
##' @param iter The row number of uniqueItem that should be processed.
##' @param removePriorImputation Logical.  Should previous imputation estimates 
##'   be removed?
##' @param appendName Character.  This text will be appended to the file name, 
##'   and is useful for distinguishing between runs with different parameters.
##' @param ensemModels A list of ensemble models (i.e. objects of type 
##'   faoswsImputation::ensembleModel).  If NULL, the default is used.
##'   
##' @return A single row data.frame with: the CPC item code, a logical value
##'   indicating if the imputation was successful, and an error message (or
##'   blank if no error occurred).
##' 
runModel = function(iter, removePriorImputation, appendName = "",
                    ensemModels = NULL){
    singleItem = uniqueItem[iter, measuredItemCPC]
    isPrimary = uniqueItem[iter, isPrimary]
    subKey@dimensions$measuredItemCPC@keys = singleItem
    print(paste0("Imputation for item: ", singleItem))
    
    impute = try({
        cat("Reading in the data...\n")
        datasets = getProductionData(dataContext = subKey)
        ## NOTE (Michael): The yield should have been
        ##                 calculated a priori to the
        ##                 imputation module.
        
        ## Some commodities have multiple formulas.  For example, the LNSP 
        ## (livestock non-primary) item type has beef, indigenous beef, and 
        ## biological beef.  Rather than being different commodities, these 
        ## three commodities are stored under the beef commodity with different 
        ## element codes for production/yield/output.  So, we need to process 
        ## each one and thus loop over all the formulaTuples (which specifies 
        ## the multiple element codes if applicable).  In this looping, we'll 
        ## filter datasets$query as required, so we need to copy and save it
        ## here.
        savedData = copy(datasets$query)
        for(i in 1:nrow(datasets$formulaTuples)){
            cat("Processing pair", i, "of", nrow(datasets$formulaTuples),
                "element triples.\n")
            datasets$query = copy(savedData)
            datasets = cleanData(datasets, i = i)
            
            ## Impute the yield
            processingParams = defaultProcessingParameters(
                productionValue = datasets$formulaTuples[, output][i],
                yieldValue = datasets$formulaTuples[, productivity][i],
                areaHarvestedValue = datasets$formulaTuples[, input][i])
            p = getImputationParameters(datasets, i = i)
            ## NOTE (Michael): Stop the plotting.
            p$yieldParams$plotImputation = ""
            p$productionParams$plotImputation = ""
            if(!is.null(ensemModels)){
                p$yieldParams$ensembleModels = ensemModels
                p$productionParams$ensembleModels = ensemModels
            }
            yieldParams = p$yieldParams
            productionParams = p$productionParams
            
            ## Original design was to remove old estimated imputation values. 
            ## Salar then requested to keep them, and then we decided to remove
            ## them again.  We remove here before imputing and then keep
            ## imputations.  This allows the production imputation to use the
            ## newly imputed yield data, otherwise it would all be ignored.     
            processingParams$removePriorImputation = removePriorImputation
            processingParams$imputedFlag = c("I", "E")
            datasets$query = processProductionDomain(data = datasets$query,
                                    processingParameters = processingParams)
            processingParams$removePriorImputation = FALSE
            processingParams <<- processingParams
            imputationParams <<- p
            if(isPrimary){
                ## Commented because it's done inside buildEnsembleModel
                ## cat("Computing yield...\n")
                ## datasets$query = processProductionDomain(datasets$query,
                ##                                          processingParams)
                ## computeYield(datasets$query, newMethodFlag = "i",
                ##              processingParameters = processingParams,
                ##              unitConversion = datasets$formulaTuples[i, unitConversion])

                datasets$query[, countryCnt := .N, by = c(processingParams$byKey)]
                datasets$query = datasets$query[countryCnt > 1, ]
                datasets$query[, countryCnt := NULL]
                cat("Imputing yield...\n")
                rawData <<- copy(datasets$query)
                originalData = copy(datasets$query)
                ## Useful when running locally to examine model results:
                ## png(paste0(R_SWS_SHARE_PATH, "/browningj/production/imputationPlots/yield_",
                ##                    singleItem, "_", i, ".png"), width = 2300, height = 2300)
                modelYield = try(buildEnsembleModel(
                    data = datasets$query, imputationParameters = yieldParams,
                    processingParameters = processingParams,
                    unitConversion = datasets$formulaTuples[i, unitConversion]))
                # dev.off()
                modelYield$fit[, variance := NULL]
                setnames(modelYield$fit, "fit", "Value")
                modelYield$fit[, measuredElement :=
                                   datasets$formulaTuples[i, productivity]]
                modelYield$fit[, flagObservationStatus := "I"]
                modelYield$fit[, flagMethod := "e"]
                
                ## Use original data to figure out which observations were
                ## updated (not imputed, but updated via compute yield)
                ##
                ## DANGER (Michael): In the buildEnsembleModel
                ##                   function, the yield computation
                ##                   result is directly updated in the
                ##                   raw data set. Thus we need to
                ##                   identify the set which was
                ##                   updated compare to the old
                ##                   value. This side effect should be
                ##                   eliminated.
                diffs = getUpdatedObs(dataOld = originalData,
                                      dataNew = datasets$query,
                                      key = c(yieldParams$byKey,
                                              processingParams$yearValue),
                                      wideVarName = "measuredElement")
                diffs <<- diffs
                modelYield <<- modelYield
                modelYield$fit = rbind(modelYield$fit, diffs)
                allYieldComputation <<- modelYield
            } else {
                ## No yield model if imputing derived, as we just impute on the
                ## production time series.
                modelYield = NULL
                diffs = NULL
            }

            ## NOTE (Michael): The logic of the following ifelse is:
            ##
            ## (1) if the commodity is primary, but yield was not
            ##     imputed, then we just balance production and assign
            ##     NULL to the yield model.
            ##
            ## (2) if the commodity is primary, and the yield values
            ##     were imputed, then update the yield values then
            ##     blanace production.
            ##
            ## (3) If both failed, then assign NULL to the yield model.

            if(isPrimary & is(modelYield, "numeric")){
                ## modelYield **should** be a list, but if no data is missing a 
                ## vector is returned.  In this case, just balance and continue.
                save(savedData,
                     file = paste0("checkProductionImputation/notCreated/prodModel_",
                                   singleItem, "_", i, appendName, "yield.RData"))
                balanceProduction(data = datasets$query,
                                  processingParameters = processingParams)
                modelYield = NULL
            } else if(isPrimary & !is(modelYield, "try-error")){
                ## In this case, we have the expected behaviour: modelYield is a
                ## list.
                yieldVar = paste0("Value_measuredElement_",
                                  datasets$formulaTuples[i, productivity])
                ## Yield must be zero if production or area harvested are 0.
                ##
                ## NOTE (Michael): The above comment is wrong, the
                ##                 yield should be missing!
                zeroYield = datasets$query[
                    get(processingParams$areaHarvestedValue) == 0 |
                    get(processingParams$productionValue) == 0,
                    c(processingParams$yearValue, yieldParams$byKey), with = FALSE]
                modelYield[[1]] =
                    modelYield[[1]][!zeroYield, on = c(processingParams$yearValue,
                                                       yieldParams$byKey)]
                ## Yield should not be imputed on 0Mn observations.  These are
                ## "missing but assumed negligble."
                ##
                ## NOTE (Michael): WHY?!? We should impute where available.
                assumedZero = datasets$query[
                    (get(processingParams$yieldValue) == 0 |
                     is.na(get(processingParams$yieldValue))) &
                    get(processingParams$yieldMethodFlag) %in% c("-", "n") &
                    get(processingParams$yieldObservationFlag) == "M",
                    c(processingParams$yearValue, yieldParams$byKey), with = FALSE]
                modelYield[[1]] =
                    modelYield[[1]][!assumedZero, on = c(processingParams$yearValue, yieldParams$byKey)]
                modifiedModelYield <<- modelYield
                ## Have to save the yield estimates to the data because we need
                ## to balance and then impute production.  Also, check if fit is
                ## NULL because it throws an error if no observations are
                ## missing and estimated.
                if(!is.null(modelYield$fit$Value)){
                    toMerge = copy(modelYield$fit)
                    setnames(toMerge, c("Value", "flagObservationStatus",
                                        "flagMethod"),
                             c("Value_new", "flag_new", "method_new"))
                    ## NOTE (Michael): Overwirting existing values
                    ##                 with imputed values.
                    datasets$query[toMerge,
                                   c(yieldVar,
                                     yieldParams$imputationFlagColumn,
                                     yieldParams$imputationMethodColumn) :=
                                       list(Value_new, flag_new, method_new),
                                   on = c("timePointYears", "geographicAreaM49",
                                          "measuredItemCPC"), all = TRUE]
                }
                originalData = copy(datasets$query)
                balanceProduction(data = datasets$query,
                                  processingParameters = processingParams,
                                  unitConversion =
                                      datasets$formulaTuples[i, unitConversion])
                ## Use original data to figure out which observations were
                ## updated (not imputed, but updated via compute yield)
                ##
                ## NOTE (Michael): The above comment is incorrect,
                ##                 this step is checking which
                ##                 production is updated through
                ##                 balanceProduction.
                diffs = getUpdatedObs(dataOld = originalData,
                                      dataNew = datasets$query,
                                      key = c(yieldParams$byKey,
                                              processingParams$yearValue),
                                      wideVarName = "measuredElement")
                prodDiff <<- diffs
                ## NOTE (Michael): The modelYield$fit actually
                ##                 contains the computation of yield,
                ##                 imputation of yield nad then the
                ##                 balance of production.
                modelYield$fit = rbind(modelYield$fit, diffs)
            } else {
                ## If model building failed, we still want to continue in case
                ## we can build a production model.
                ##
                ## NOTE (Michael): Should investigate why the model failed.
                modelYield = NULL
            }
            
            ## Impute production
            cat("Imputing production...\n")
            originalData = copy(datasets$query)
            ## Useful when running locally to examine model results:
            ## png(paste0(R_SWS_SHARE_PATH, "/browningj/production/imputationPlots/production_",
            ##        singleItem, "_", i, ".png"), width = 2300, height = 2300)
            ## Use try to make sure that we call dev.off() before exiting.
            ## 
            ## NOTE(Michael): Looks like the production model is assumed to never fail.
            modelProduction = buildEnsembleModel(
                data = datasets$query, imputationParameters = productionParams,
                processingParameters = processingParams,
                unitConversion = datasets$formulaTuples[i, unitConversion])
            modelProduction$fit[, variance := NULL]
            setnames(modelProduction$fit, "fit", "Value")
            modelProduction$fit[, measuredElement :=
                                    datasets$formulaTuples[i, output]]
            modelProduction$fit[, flagObservationStatus := "I"]
            modelProduction$fit[, flagMethod := "e"]
            diffs = getUpdatedObs(dataOld = originalData,
                                  dataNew = datasets$query,
                                  key = c(yieldParams$byKey,
                                          processingParams$yearValue),
                                  wideVarName = "measuredElement")
            modelProduction$fit = rbind(modelProduction$fit, diffs)
            ## dev.off()
            ## Production must be zero if area harvested is 0.
            zeroProd = datasets$query[get(processingParams$areaHarvestedValue) == 0,
                                      c(processingParams$yearValue, productionParams$byKey),
                                      with = FALSE]
            modelProduction[[1]] =
                modelProduction[[1]][!zeroProd,
                                     on = c(processingParams$yearValue, productionParams$byKey)]
            ## Production should not be imputed on 0Mn observations.  These are 
            ## "missing but assumed negligble."  Additionally, we have not been 
            ## able to identify 0M from the old system as 0Mu or 0Mn and have
            ## thus assigned them the flags 0M-.  These should be treated as
            ## 0Mn in this case.
            assumedZero = datasets$query[(get(processingParams$productionValue) == 0 |
                                          is.na(get(processingParams$productionValue))) &
                                         get(processingParams$productionMethodFlag) %in% c("-", "n") &
                                         get(processingParams$productionObservationFlag) == "M",
                                         c(processingParams$yearValue, productionParams$byKey),
                                         with = FALSE]
            modelProduction[[1]] =
                modelProduction[[1]][!assumedZero,
                                     on = c(processingParams$yearValue, productionParams$byKey)]

            ## Update the differences that would occur when running the compute
            ## yield module at the end.
            ## 
            ## We have to save the production estimates to the data because we
            ## need to balance.  Also, check if fit is NULL because it throws an
            ## error if no observations are missing and estimated.
            if(!is.null(modelProduction$fit$Value)){
                productionVar = paste0("Value_measuredElement_",
                                       datasets$formulaTuples[i, output])
                toMerge = copy(modelProduction$fit)
                setnames(toMerge, c("Value", "flagObservationStatus",
                                    "flagMethod"),
                         c("Value_new", "flag_new", "method_new"))
                datasets$query[toMerge,
                               c(productionVar,
                                 productionParams$imputationFlagColumn,
                                 productionParams$imputationMethodColumn) :=
                                   list(Value_new, flag_new, method_new),
                               on = c("timePointYears", "geographicAreaM49",
                                      "measuredItemCPC"), all = TRUE]
            }
            originalData = copy(datasets$query)
            balanceAreaHarvested(data = datasets$query,
                processingParameters = processingParams,
                unitConversion = datasets$formulaTuples[i, unitConversion])
            diffs = getUpdatedObs(dataOld = originalData,
                                  dataNew = datasets$query,
                                  key = c(yieldParams$byKey,
                                          processingParams$yearValue),
                                  wideVarName = "measuredElement")
            areaDiff <<- diffs
            modelProduction$fit = rbind(modelProduction$fit, diffs)
            cat("Trying to save data\n")
            ## Save models
            checkData <<-
                list(modelYield = modelYield,
                     modelProduction = modelProduction,
                     years = years,
                     finalImputedData = copy(datasets$query))


            checkAllValuesImputed = try({
                lapply(grep("Value", colnames(checkData$finalImputedData),
                            value = TRUE),
                       FUN = function(valueColumn){
                           checkTimeSeriesImputed(checkData$finalImputedData,
                                                  key = areaVar,
                                                  valueColumn = valueColumn)
                       })
            })
            if(inherits(checkAllValuesImputed, "try-error")){
                save(modelYield, modelProduction, years,
                     file = paste0("checkProductionImputation/notImputed/prodModel_",
                                   singleItem, "_", i, appendName, ".RData"))
                stop("Not all values were imputed")
            }
            ## NOTE (Michael): Need to add in checks for the objects
            ##                 saved.
            save(modelYield, modelProduction, years,
                 file = paste0(saveDir, "prodModel_",
                               singleItem, "_", i, appendName, ".RData"))
        } ## close item type for loop
    }) ## close try block
    if(inherits(impute, "try-error")){
        print("Imputation Module Failed")
    } else {
        print("Imputation Module Executed Successfully")
    }
    message = ifelse(inherits(impute, "try-error"), attr(impute, "condition")$message, "")
    return(data.frame(cpc = singleItem, success = !inherits(impute, "try-error"),
               errorMessage = message))
}

simplerModels = allDefaultModels()
simplerModels = simplerModels[c("defaultMean", "defaultLm",
                                "defaultExp", "defaultNaive",
                                "defaultMixedModel")]
## For testing
## uniqueItem = uniqueItem[1, ]
if(runParallel){
    result = foreach(iter = 1:nrow(uniqueItem), .combine = rbind) %dopar% {
        runModel(iter, removePriorImputation = TRUE,
                 appendName = "_est_removed")
    }
    result2 = foreach(iter = 1:nrow(uniqueItem), .combine = rbind) %dopar% {
        runModel(iter, removePriorImputation = FALSE,
                 appendName = "_est_kept", ensemModels = simplerModels)
    }
    result = rbind(result, result2)
} else {
    result = list()
    for(iter in 1:nrow(uniqueItem)){
        ## rows = sample(nrow(uniqueItem), size = 5)
        ## for(iter in rows){
        result[[length(result) + 1]] =
            runModel(iter, removePriorImputation = TRUE,
                     appendName = "_est_removed")
        result[[length(result) + 1]] =
            runModel(iter, removePriorImputation = FALSE,
                     appendName = "_est_kept", ensemModels = simplerModels)
    }
    result = do.call("rbind", result)
}

paste0("Successfully built ", sum(result$success), " models out of ",
       nrow(result), " commodities.\n",
       "Failed commodities: ", paste(result$cpc[!result$success], collapse = ", "))


# builtModels = dir(paste0(R_SWS_SHARE_PATH, "/browningj/production/"),
#                   pattern = "prodModel*")
# builtModels = stringr::str_extract(builtModels, "[0-9.]+")
# builtModels = unique(builtModels)
# missingModels = allItemCodes[!allItemCodes %in% builtModels]

                              
