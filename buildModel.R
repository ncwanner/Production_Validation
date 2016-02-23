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
years = 1991:2014
runParallel = FALSE

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
DEBUG_MODE = Sys.getenv("R_DEBUG_MODE")

if(!exists("DEBUG_MODE") || DEBUG_MODE == ""){
    cat("Not on server, so setting up environment...\n")
    
    server = "QA"
    # server = "Prod"
    
    stopifnot(server %in% c("QA", "Prod"))
    ## Define directories
    if(Sys.info()[7] == "josh"){
        apiDirectory = "~/Documents/Github/faoswsProduction/R/"
        R_SWS_SHARE_PATH = ifelse(server == "Prod", "/media/hqlprsws2_prod/",
                                  "/media/hqlprsws1_qa/")
        # runParallel = TRUE
    } else if(Sys.info()[7] == "rockc_000"){
        apiDirectory = "~/Github/faoswsProduction/R/"
        stop("Can't connect to share drives!")
    }

    ## Get SWS Parameters
    SetClientFiles(dir = ifelse(server == "Prod",
                                "~/R certificate files/Production/",
                                "~/R certificate files/QA/"))
    if(server == "Prod"){
        GetTestEnvironment(
            baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
            token = "b3c0a795-29dd-45e9-9b96-175a9f19f7f9"
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
saveDir = paste0(R_SWS_SHARE_PATH, "/browningj/production/")

fullKey = getMainKey(years = years)

subKey = fullKey
uniqueItem = fullKey@dimensions$measuredItemCPC@keys
uniqueItem = data.table(measuredItemCPC = uniqueItem)
uniqueItem[, isPrimary := grepl("^0", measuredItemCPC)]
warning("Primary items should be identified with a table in the SWS!!!")

completed = dir(saveDir, pattern = "^prodModel.*\\.RData$")
completed = gsub("prodModel_|_[1-9]\\.RData", "", completed)
uniqueItem = uniqueItem[!measuredItemCPC %in% completed, ]

runModel = function(iter){
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
            yieldParams = p$yieldParams
            yieldParams$ensembleModels$defaultLogistic@model = defaultLogistic
            productionParams = p$productionParams
            productionParams$ensembleModels$defaultLogistic@model = defaultLogistic

            if(isPrimary){
                cat("Computing yield...\n")
                ## Don't remove old estimated imputation values, per request from
                ## Salar.
                processingParams$removePriorImputation = FALSE
                computeYield(datasets$query, newMethodFlag = "i",
                             processingParameters = processingParams)

                datasets$query[, countryCnt := .N, by = c(processingParams$byKey)]
                datasets$query = datasets$query[countryCnt > 1, ]
                datasets$query[, countryCnt := NULL]
                cat("Imputing yield...\n")
                ## Useful when running locally to examine model results:
                # png(paste0(R_SWS_SHARE_PATH, "/browningj/production/imputationPlots/yield_",
                #                    singleItem, "_", i, ".png"), width = 2300, height = 2300)
                modelYield = try(buildEnsembleModel(
                    data = datasets$query, imputationParameters = yieldParams,
                    processingParameters = processingParams))
                # dev.off()
            } else {
                ## No yield model if imputing derived, as we just impute on the
                ## production time series.
                modelYield = NULL
            }
            if(isPrimary & is(modelYield, "numeric")){
                ## modelYield **should** be a list, but if no data is missing a 
                ## vector is returned.  In this case, just balance and continue.
                balanceProduction(data = datasets$query,
                                  processingParameters = processingParams)
                modelYield = NULL
            } else if(isPrimary & !is(modelYield, "try-error")){
                ## In this case, we have the expected behaviour: modelYield is a
                ## list.
                yieldVar = paste0("Value_measuredElement_",
                                  datasets$formulaTuples$productivity)
                ## Yield must be zero if production or area harvested are 0.
                zeroYield = datasets$query[
                    get(processingParams$areaHarvestedValue) == 0 |
                    get(processingParams$productionValue) == 0,
                    c(processingParams$yearValue, yieldParams$byKey), with = FALSE]
                modelYield[[1]] =
                    modelYield[[1]][!zeroYield, on = c(processingParams$yearValue,
                                                       yieldParams$byKey)]
                ## Yield should not be imputed on 0Mn observations.  These are
                ## "missing but assumed negligble."
                assumedZero = datasets$query[
                    (get(processingParams$yieldValue) == 0 |
                     is.na(get(processingParams$yieldValue))) &
                    get(processingParams$yieldMethodFlag) %in% c("-", "n") &
                    get(processingParams$yieldObservationFlag) == "M",
                    c(processingParams$yearValue, yieldParams$byKey), with = FALSE]
                modelYield[[1]] =
                    modelYield[[1]][!assumedZero, on = c(processingParams$yearValue, yieldParams$byKey)]
                ## Have to save the yield estimates to the data because we need
                ## to balance and then impute production.  Also, check if fit is
                ## NULL because it throws an error if no observations are
                ## missing and estimated.
                if(!is.null(modelYield$fit$fit)){
                    datasets$query[modelYield$fit,
                                   c(yieldVar,
                                     yieldParams$imputationFlagColumn,
                                     yieldParams$imputationMethodColumn) :=
                                       list(fit, "I", "e"),
                                   on = c("timePointYears", "geographicAreaM49",
                                          "measuredItemCPC"), all = TRUE]
                }
                balanceProduction(data = datasets$query,
                                  processingParameters = processingParams)
            } else {
                ## If model building failed, we still want to continue in case
                ## we can build a production model.
                modelYield = NULL
            }
            
            ## Impute production
            cat("Imputing production...\n")
            ## Useful when running locally to examine model results:
            # png(paste0(R_SWS_SHARE_PATH, "/browningj/production/imputationPlots/production_",
            #        singleItem, "_", i, ".png"), width = 2300, height = 2300)
            ## Use try to make sure that we call dev.off() before exiting.
            modelProduction = try(buildEnsembleModel(
                data = datasets$query, imputationParameters = productionParams,
                processingParameters = processingParams))
            # dev.off()
            if(is(modelProduction, "try-error")){
                stop(attr(modelProduction, "condition")$message)
            }
            ## Production must be zero if area harvested is 0.
            zeroProd = datasets$query[get(processingParams$areaHarvestedValue) == 0,
                                      c(processingParams$yearValue, productionParams$byKey),
                                      with = FALSE]
            modelProduction[[1]] =
                modelProduction[[1]][!zeroProd,
                                     on = c(processingParams$yearValue, productionParams$byKey)]
            ## Round production values to whole numbers
            modelProduction[[1]][, fit := round(fit)]
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

            ## Save models
            save(modelYield, modelProduction, years,
                 file = paste0(saveDir, "prodModel_",
                               singleItem, "_", i, ".RData"))
        } # close item type for loop
    }) # close try block
    if(inherits(impute, "try-error")){
        print("Imputation Module Failed")
    } else {
        print("Imputation Module Executed Successfully")
    }
    message = ifelse(inherits(impute, "try-error"), attr(impute, "condition")$message, "")
    return(data.frame(cpc = singleItem, success = !inherits(impute, "try-error"),
               errorMessage = message))
}

if(runParallel){
    result = foreach(iter = 1:nrow(uniqueItem), .combine = rbind) %dopar% {
        runModel(iter)
    }
} else {
    result = list()
    for(iter in 1:nrow(uniqueItem)){
        result[[length(result) + 1]] = runModel(iter)
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