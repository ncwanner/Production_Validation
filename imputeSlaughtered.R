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
expandMeatSessionSelection = function(oldKey, selectedMeat){
    rowsIncluded =
        selectedMeat[, measuredItemParentCPC %in%
                       oldKey@dimensions$measuredItemCPC@keys |
                       measuredItemChildCPC %in%
                       oldKey@dimensions$measuredItemCPC@keys]
    requiredMeats =
        selectedMeat[rowsIncluded, c(measuredItemParentCPC, measuredItemChildCPC)]
    oldKey@dimensions[[itemVar]]@keys = requiredMeats
    if(length(oldKey@dimensions$measuredItemCPC@keys) == 0){
        stop("No meat/animal commodities are in the session, and thus this ",
             "module has nothing to do.")
    }

    ## Create a copy to update the key
    newKey = oldKey
    ## Update the measuredElements
    newKey@dimensions[[elementVar]]@keys =
        unique(selectedMeat[rowsIncluded,
                            c(measuredElementParent, measuredElementChild)])
    
    ## Adjust the years based on the passed information:
    newKey@dimensions[[yearVar]]@keys =
        as.character(firstDataYear:lastYear)

    ## Include all countries, since all data is required for the imputation
    countryCodes = GetCodeList("agriculture", "aproduction", "geographicAreaM49")
    newKey@dimensions[[areaVar]]@keys = countryCodes[type == "country", code]
    newKey
}

## Create the new expanded key
newKey = expandMeatSessionSelection(oldKey = swsContext.datasets[[1]],
                                    selectedMeat = selectedMeat)


transferAnimalNumber = function(data, selectedMeat){
    dataCopy = copy(data)
    ## Step 1. Transfer down the slaughtered animal numbers from the
    ##         animal (parent) commodity to the meat (child) commodity.

    ## Remove missing values, as we don't want to copy those.
    parentData = dataCopy[!flagObservationStatus == "M" &
                          measuredItemCPC %in% selectedMeat$measuredItemParentCPC &
                          timePointYears <= lastYear &
                          timePointYears >= firstDataYear, ]
    setnames(parentData, c(itemVar, elementVar),
             c("measuredItemParentCPC", "measuredElementParent"))
    ## NOTE (Michael): This should not be called child data, since there
    ##                 is no child data, it is parent data but with the
    ##                 mapping table and variable names changed.
    childData = merge(parentData, selectedMeat,
                      by = c("measuredItemParentCPC", "measuredElementParent"))
    childData[, c("measuredItemParentCPC", "measuredElementParent") := NULL]
    setnames(childData, c("measuredItemChildCPC", "measuredElementChild"),
             c(itemVar, elementVar))


    ## The (childData) data frame contains the value of the parent
    ## commodity, while the (data) dataframe contains all the data. After
    ## the merge, we over write the values in (data) from the values in
    ## (childData).
    ##
    ## NOTE (Michael): The merge should be set to all.y = TRUE and only
    ##                 restrict to the set that should be over-written.
    dataMerged = merge(dataCopy, childData, all = TRUE, suffixes = c("", ".new"),
                       by = c(areaVar, itemVar, elementVar, yearVar))
    dataMerged[!is.na(Value.new),
               c("Value", "flagObservationStatus", "flagMethod") :=
               list(Value.new, flagObservationStatus.new, flagMethod.new)]
    dataMerged[, c("Value.new", "flagObservationStatus.new",
                   "flagMethod.new") := NULL]
    dataMerged
}

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

            removeSingleEntryCountry = function(data,
                                            params =
                                                defaultProcessingParameters()){
                ## NOTE(Michael): This function makes imputation of
                ##                all data not possible, as country
                ##                with only one observation will not
                ##                be imputed.
                dataCopy = copy(data)
                dataCopy[, countryCnt := .N, by = c(params$byKey)]
                dataCopy = dataCopy[countryCnt > 1, ]
                dataCopy[, countryCnt := NULL]
                dataCopy
            }
            cleanedData$query =
                removeSingleEntryCountry(cleanedData$query,
                                         params = processingParams)

            
            ## Determine which observations shouldn't be imputed:
            ## Production should not be imputed on 0Mn observations.  These are 
            ## "missing but assumed negligble."  Additionally, we have not been 
            ## able to identify 0M from the old system as 0Mu or 0Mn and have
            ## thus assigned them the flags 0M-.  These should be treated as
            ## 0Mn in this case.

            getForcedZeroKey = function(data,
                                        processingParams =
                                            defaultProcessingParameters(),
                                        productionParams =
                                            getImputationParameters()$productionParams
                                        ){

                assumedZero =
                    data[(get(processingParams$productionValue) == 0 |
                          is.na(get(processingParams$productionValue))) &
                         get(processingParams$productionMethodFlag) %in%
                         c("-", "n") &
                         get(processingParams$productionObservationFlag) == "M",
                         c(processingParams$yearValue, productionParams$byKey),
                         with = FALSE]
                ## Production must be zero if area harvested is 0.
                zeroProd =
                    data[get(processingParams$productionValue) == 0 &
                         get(processingParams$productionObservationFlag) != "M",
                         c(processingParams$yearValue, productionParams$byKey),
                         with = FALSE]
                zeroArea =
                    data[get(processingParams$areaHarvestedValue) == 0 &
                         get(processingParams$areaHarvestedObservationFlag) != "M",
                         c(processingParams$yearValue, productionParams$byKey),
                         with = FALSE]
                forcedZero = unique(rbind(assumedZero, zeroProd, zeroArea))
                forcedZero
            }

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
    ## Step 3. Copy the slaughtered animal numbers in meat back to the
    ##         animal commodity.
    transferSlaughteredNumber = function(preUpdatedData, imputationResult){
        childData = copy(imputationResult)
        setnames(childData, c(itemVar, elementVar),
                 c("measuredItemChildCPC", "measuredElementChild"))
        parentData = merge(childData, toProcess,
                           by = c("measuredItemChildCPC", "measuredElementChild"))
        parentData[, c("measuredItemChildCPC", "measuredElementChild") := NULL]
        setnames(parentData, c("measuredItemParentCPC", "measuredElementParent"),
                 c(itemVar, elementVar))
        parentData[, timePointYears := as.character(timePointYears)]

        ## Only need to keep the updated data
        updatedData = merge(preUpdatedData, parentData, all.y = TRUE,
                            suffixes = c("", ".new"),
                            by = c(areaVar, itemVar, elementVar, yearVar))
        updatedData = updatedData[is.na(Value) & !is.na(Value.new), ]
        updatedData[, c("Value", "flagObservationStatus", "flagMethod") :=
                    list(Value.new, flagObservationStatus.new, flagMethod.new)]
        updatedData[, c("Value.new", "flagObservationStatus.new",
                        "flagMethod.new") := NULL]
        updatedData
    }
    
    ## Step 4. Save all three variables for meat (production/animals
    ##         slaughterd/carcass weight) and the animals slaughtered
    ##         for the animal.
    ## 
    ## Note (Michael): The above comment is incorrect, only the animal
    ##                 number is saved back to the animal commdotiy.

    ## Module Testing before saving the data back to the database
    saveResult =
        transferSlaughteredNumber(step1Data, result) %>%
        checkTimeSeriesImputed(dataToBeSaved = .,
                               key = c("geographicAreaM49",
                                       "measuredItemCPC", "measuredElement"),
                               valueColumn = "Value") %>%
        checkProtectedData(dataToBeSaved = .) %>%
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
