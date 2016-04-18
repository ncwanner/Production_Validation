## ########################################################################### 
## Title: Impute Slaughtered Module for SWS
## 
## Author: Josh Browning
## 
## The animals slaughtered for production of meat, offals, fats and hides must 
## be available before running the production imputation code.  These numbers, 
## however, are not guaranteed to be available, and in the case of missing data,
## an imputation method must be applied.  The decision was to use the production
## figures of meat, if available, to compute the missing animals slaughtered. If
## these figures are also missing, they should be imputed using the production 
## imputation methodology.  Of course, in the case of currently available data 
## in the animal element, that data should be transferred to the quantity of 
## animals slaughtered for meat and then the imputation ran.  We also decided to
## save the imputations for meat so as to retain consistency with the animal 
## figures.
##
## The steps are as follows:
## 0. Transfer down the slaughtered animal numbers from the animal (parent)
## commodity to the meat (child) commodity.
## 1. Save the transfered data back to the database
## 2. Impute the meat data (production/animals slaughtered/carcass weight)
## following the logic from the production imputation module.
## 3. Copy the slaughtered animal numbers in meat back to the animal commodity.
## 4. Save all three variables for meat (production/animals slaughterd/carcass
## weight) and the animals slaughtered for the animal.
## #############################################################################

## Step 0. Initial set-up

cat("Beginning impute slaughtered script...\n")

library(data.table)
library(faosws)
library(faoswsFlag)
library(faoswsUtil)
library(faoswsImputation)
library(splines)
library(magrittr)

areaVar = "geographicAreaM49"
yearVar = "timePointYears"
itemVar = "measuredItemCPC"
elementVar = "measuredElement"
yearsModeled = 20
minObsForEst = 5
impFlags = c("I", "E")
missFlags = "M"
## server is only used for debug sessions:
##server = "Prod"
server = "QA"

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
DEBUG_MODE = Sys.getenv("R_DEBUG_MODE")

if(!exists("DEBUG_MODE") || DEBUG_MODE == ""){
    cat("Not on server, so setting up environment...\n")

    if(Sys.info()[7] == "josh"){ # Josh work
        files = dir("~/Documents/Github/faoswsProduction/R/",
                    full.names = TRUE)
        if(server == "Prod"){
            SetClientFiles("~/R certificate files/Production/")
            R_SWS_SHARE_PATH = "/media/hqlprsws2_prod/"
            url = "https://hqlprswsas1.hq.un.fao.org:8181/sws"
        } else {
            SetClientFiles("~/R certificate files/QA/")
            R_SWS_SHARE_PATH = "/media/hqlprsws1_qa/"
            url = "https://hqlqasws1.hq.un.fao.org:8181/sws"
        }
    } else if(Sys.info()[7] == "mk"){ # Josh work
        files = dir("R/", full.names = TRUE)
        if(server == "Prod"){
            SetClientFiles("~/.R/prod/")
            R_SWS_SHARE_PATH = "/media/sws_prod_shared_drive/"
            url = "https://hqlprswsas1.hq.un.fao.org:8181/sws"
        } else {
            SetClientFiles("~/.R/qa/")
            R_SWS_SHARE_PATH = "/media/sws_qa_shared_drive/"
            url = "https://hqlqasws1.hq.un.fao.org:8181/sws"
        }
    } else {
        stop("Add your github directory here!")
    }

    ## Get SWS Parameters
    if(server == "Prod"){
        GetTestEnvironment(
            baseUrl = url,
            token = "2620c6fd-05b2-48ef-b348-61097ed539b6"
        )
    } else if(server == "QA"){
        GetTestEnvironment(
            baseUrl = url,
            token = "f8646896-2ed2-4e88-9cd2-9db6d735991f"
        )
    }
    sapply(files, source)
} else {
    cat("Working on SWS...\n")
}

## Just testing 1 item
## swsContext.datasets[[1]]@dimensions[[itemVar]]@keys = "21111.01" ## meat of cattle
## swsContext.datasets[[1]]@dimensions[[itemVar]]@keys = "21113.01" ## meat of pig

startTime = Sys.time()

message("Loading preliminary data...\n")
firstYear = as.numeric(swsContext.computationParams$firstYear)
lastYear = as.numeric(swsContext.computationParams$lastYear)
firstDataYear = lastYear - yearsModeled + 1
stopifnot(firstDataYear <= firstYear)
stopifnot(firstYear <= lastYear)

toProcess = getAnimalMeatMapping()
toProcess[, c("Item Name", "Child Item Name") := NULL]
## Filter to just meats => CPC code like 2111* or 2112* (21111.01, 21112, ...)
selectedMeat = toProcess[grepl("^211(1|2|7).*", measuredItemChildCPC), ]

## Read the data.  The years and countries provided in the session are
## used, and the commodities in the session are somewhat
## considered. For example, if 02111 (Cattle) is in the session, then
## the session will be expanded to also include 21111.01 (meat of
## cattle, fresh or chilled), 21151 (edible offal of cattle, fresh,
## chilled or frozen), 21512 (cattle fat, unrendered), and 02951.01
## (raw hides and skins of cattle).  The measured element dimension of
## the session is simply ignored.

## Expand the session to include missing meats
newKey = expandMeatSessionSelection(oldKey = swsContext.datasets[[1]],
                                    selectedMeat = selectedMeat)

## Execute the get data call.
cat("Pulling the data...\n")

step1Data = 
    newKey %>%
    GetData(key = .) %>%
    ## NOTE (Michael): Should add preProcessing here
    transferAnimalNumber(data = ., selectedMeat)

## Module test and save the transfered data back
step1Data %>%
    checkProtectedData(dataToBeSaved = .) %>%
    SaveData("agriculture", "aproduction", data = .)



## Step 2. Impute the meat data (production/animals
##         slaughtered/carcass weight) following the logic from the
##         production imputation module.
##
## NOTE (Michael): The imputed data for meat triplet is also saved
##                 back in this step.

uniqueItem = newKey@dimensions$measuredItemCPC@keys
                                        # Just impute the meat elements
uniqueItem = uniqueItem[uniqueItem %in% toProcess$measuredItemChildCPC]
uniqueItem = as.character(uniqueItem)


result = NULL
successCount = 0
failCount = 0
for(iter in 1:length(uniqueItem)){
    singleItem = uniqueItem[iter]
    subKey = newKey
    subKey@dimensions$measuredItemCPC@keys = singleItem
    print(paste0("Imputation for item: ", singleItem))
    
    impute = try({
        cat("Reading in the data...\n")
        datasets = getProductionData(subKey)
        ## Ignore indigenous/biological:
        datasets$formulaTuples = datasets$formulaTuples[nchar(input) == 4, ]
        
        for(i in 1:nrow(datasets$formulaTuples)){
            ## For convenience, let's save the formula tuple to "filter"
            filter = datasets$formulaTuples[i, ]
            message("Processing pair ", i, " of ", nrow(datasets$formulaTuples),
                    " element triples.")
            cleanedData = cleanData(datasets, i = i, maxYear = 2014)
            
            ## Setup for the imputation
            processingParams = defaultProcessingParameters(
                productionValue = filter[, output],
                yieldValue = filter[, productivity],
                areaHarvestedValue = filter[, input])
            processingParams$imputedFlag = c("I", "E")
            p = getImputationParameters(cleanedData, i = i)
            ## NOTE (Michael): Stop the plotting.
            p$yieldParams$plotImputation = ""
            p$productionParams$plotImputation = ""
            yieldParams = p$yieldParams
            productionParams = p$productionParams

            
            cleanedData$query =
                removeSingleEntryCountry(cleanedData$query,
                                         params = processingParams)

            forcedZero =
                getForcedZeroKey(cleanedData$query,
                                 processingParam = processingParams,
                                 productionParams = productionParams)

            
            ## Imputation is a bit tricky, as we want to exclude previously 
            ## estimated data if we have "enough" official/semi-official data in
            ## a time series but we want to use estimates if we don't have 
            ## enough official/semi-official data.  "Enough" is specified by
            ## minObsForEst.
            flags = paste0(cleanedData$prefixTuples$flagObsPrefix,
                           filter[, c("productivity", "output"), with = FALSE])
            validObsCnt =
                cleanedData$query[, list(yield = sum(!get(flags[1]) %in% c(impFlags, missFlags)),
                                         prod = sum(!get(flags[2]) %in% c(impFlags, missFlags))),
                                  by = geographicAreaM49]
            validObsCnt = melt(validObsCnt, id.vars = "geographicAreaM49")
            validObsCnt[, useEstimates := value < minObsForEst]
            validObsCnt[, measuredElement :=
                              ifelse(variable == "yield", filter[, productivity],
                                     filter[, output])]
            validObsCnt[, c("variable", "value") := NULL]
            
            ## For the actual imputation, we must pass all the data (as the 
            ## global models should use all the information available). 
            ## However, we'll have to delete some of the imputations
            ## (corresponding to series without enough official data) and then
            ## rerun the imputation.
            origData = copy(cleanedData$query)
            processingParams$removePriorImputation = TRUE
            cat("Imputation without Manual Estimates\n")
            imputation1 =
                imputeProductionDomain(copy(origData),
                                       processingParameters = processingParams,
                                       yieldImputationParameters = yieldParams,
                                       productionImputationParameters = productionParams,
                                       unitConversion = filter[, unitConversion])
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
                                       productionImputationParameters = productionParams,
                                       unitConversion = filter[, unitConversion])
            
            ## Take all the I/e values that have just been estimated, but don't
            ## include I/i (as balanced observations may not be correct, because
            ## we could use estimates for yield imputation and not for
            ## production, for example).
            valuesImputed1 = getUpdatedObs(
                dataOld = origData, dataNew = imputation1,
                key = c("geographicAreaM49", "timePointYears"),
                wideVarName = "measuredElement")
            valuesImputed1 = valuesImputed1[!flagMethod == "i", ]
            ## Filter to only include series with enough data:
            valuesImputed1 = merge(valuesImputed1, validObsCnt[!(useEstimates), ],
                                   by = c("geographicAreaM49", "measuredElement"))
            ## Now add in the values imputed in the second round
            valuesImputed2 = getUpdatedObs(
                dataOld = origData, dataNew = imputation2,
                key = c("geographicAreaM49", "timePointYears"),
                wideVarName = "measuredElement")
            valuesImputed2 = valuesImputed2[!flagMethod == "i", ]
            valuesImputed2 = merge(valuesImputed2, validObsCnt[(useEstimates), ],
                                   by = c("geographicAreaM49", "measuredElement"))
            
            ## Bring together the estimates and reshape them:
            valuesImputed = rbind(valuesImputed1, valuesImputed2)
            valuesImputed[, useEstimates := NULL]
            ## Add in yield estimates back to original data
            toMerge = valuesImputed[measuredElement == filter[, productivity], ]
            cols = c("Value", "flagObservationStatus", "flagMethod")
            newCols = paste0(cols, "_measuredElement_",
                             filter[, productivity])
            setnames(toMerge, cols, newCols)
            finalData = merge(origData, toMerge, all = TRUE,
                              suffixes = c("", ".new"),
                              by = c("geographicAreaM49", "timePointYears"))
            for(column in newCols){
                finalData[!is.na(get(paste0(column, ".new"))), c(column) := 
                          get(paste0(column, ".new"))]
            }
            finalData[, c(paste0(newCols, ".new"), "measuredElement") := NULL]
            ## Add in production estimates back to original data
            toMerge = valuesImputed[measuredElement == filter[, output], ]
            newCols = paste0(cols, "_measuredElement_", filter[, output])
            setnames(toMerge, cols, newCols)
            finalData = merge(finalData, toMerge, all = TRUE,
                              suffixes = c("", ".new"),
                              by = c("geographicAreaM49", "timePointYears"))
            for(column in newCols){
                finalData[!is.na(get(paste0(column, ".new"))), c(column) := 
                          get(paste0(column, ".new"))]
            }
            finalData[, c(paste0(newCols, ".new"), "measuredElement") := NULL]
            ## Now, use the identity Yield = Production / Area to add in missing
            ## values.
            computeYield(data = finalData, processingParameters = processingParams,
                         unitConversion = filter[, unitConversion])
            balanceProduction(data = finalData,
                              processingParameters = processingParams,
                              unitConversion = filter[, unitConversion])
            balanceAreaHarvested(data = finalData,
                                 processingParameters = processingParams,
                                 unitConversion = filter[, unitConversion])
            
            ## Remove the observations we don't want to impute on
            ## Use keys so we can do an anti-join
            setkeyv(forcedZero, colnames(forcedZero))
            setkeyv(finalData, colnames(forcedZero))
            finalData = finalData[!forcedZero, ]
            timeFilter = data.table(timePointYears = firstYear:lastYear,
                                    key = "timePointYears")
            setkeyv(finalData, "timePointYears")
            finalData = finalData[timeFilter, ]
            ##finalData[, ensembleVariance := NULL]
        } ## close item type for loop
        finalData
    }) ## close try block


    if(inherits(impute, "try-error")){
        message("Imputation Module Failed!")
        failCount = failCount + 1
    } else {
        step2Data = copy(impute)
        successCount = successCount + 1
        ## New module test
        impute %>%
            normalise(.) %>%
            ## Change time point year back to character
            postProcessing(data = .) %>%
            checkTimeSeriesImputed(dataToBeSaved = .,
                                   key = c("geographicAreaM49",
                                           "measuredItemCPC",
                                           "measuredElement"),
                                   valueColumn = "Value") %>%
            checkProtectedData(dataToBeSaved = .) %>%
            SaveData(domain = "agriculture", dataset = "aproduction", data = .)

        ## if(!inherits(moduleTest2, "try-error"))
        ##     SaveData("agriculture", "aproduction", data = impute,
        ##              normalized = FALSE)
        message("Imputation Module Executed Successfully!")
        ## Just need to return the numbers slaughtered code:
        impute[, paste0(c("Value", "flagObservationStatus", "flagMethod"),
                        "_measuredElement_",
                        cleanedData$formulaTuples$productivity) := NULL]
        impute[, paste0(c("Value", "flagObservationStatus", "flagMethod"),
                        "_measuredElement_",
                        cleanedData$formulaTuples$output) := NULL]
        impute[, measuredElement := cleanedData$formulaTuples$input]
        setnames(impute, colnames(impute),
                 gsub("_measuredElement_.*", "", colnames(impute)))
        result = rbind(result, impute)
    }
}


if(!is.null(result)){
    saveResult =
        ## Step 3. Copy the slaughtered animal numbers in meat back to the
        ##         animal commodity.
        transferSlaughteredNumber(step1Data, result) %>%
        ## Module Testing before saving the data back to the database
        checkTimeSeriesImputed(dataToBeSaved = .,
                               key = c("geographicAreaM49",
                                       "measuredItemCPC", "measuredElement"),
                               valueColumn = "Value") %>%
        checkProtectedData(dataToBeSaved = .) %>%
        ## Step 4. Save all three variables for meat (production/animals
        ##         slaughterd/carcass weight) and the animals slaughtered
        ##         for the animal.
        ## 
        ## Note (Michael): The above comment is incorrect, only the animal
        ##                 number is saved back to the animal commdotiy.
        SaveData(domain = "agriculture", dataset = "aproduction",
                 data = .)

    ## if(!inherits(moduleTest3, "try-error"))
    ##     saveResult = SaveData(domain = "agriculture", dataset = "aproduction",
    ##                           data = data)
}

message = paste("Module completed with", successCount,
                "commodities imputed out of", successCount + failCount)
message(message)

message
