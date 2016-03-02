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
## 1. Transfer down the slaughtered animal numbers from the animal (parent)
## commodity to the meat (child) commodity.
## 2. Impute the meat data (production/animals slaughtered/carcass weight)
## following the logic from the production imputation module.
## 3. Copy the slaughtered animal numbers in meat back to the animal commodity.
## 4. Save all three variables for meat (production/animals slaughterd/carcass
## weight) and the animals slaughtered for the animal.
## #############################################################################

## Step 0. Initial set-up

cat("Beginning impute slaughtered script...")

library(data.table)
library(faosws)
library(faoswsFlag)
library(faoswsUtil)
library(faoswsImputation)

areaVar = "geographicAreaM49"
yearVar = "timePointYears"
itemVar = "measuredItemCPC"
elementVar = "measuredElement"

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
DEBUG_MODE = Sys.getenv("R_DEBUG_MODE")

if(!exists("DEBUG_MODE") || DEBUG_MODE == ""){
    cat("Not on server, so setting up environment...\n")

    if(Sys.info()[7] == "josh"){ # Josh work
        files = dir("~/Documents/Github/faoswsProduction/R/",
                    full.names = TRUE)
        SetClientFiles("~/R certificate files/Production/")
        R_SWS_SHARE_PATH = "/media/hqlprsws2_prod/"
    } else {
        stop("Add your github directory here!")
    }

    ## Get SWS Parameters
    GetTestEnvironment(
        baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
        token = "393b08d8-d11a-46d3-ae98-ff2b1ab8fafe"
        # baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
        # token = "0289dc2c-4cf7-4c54-8954-f45a4d4b8c28"
    )
    sapply(files, source)
} else {
    cat("Working on SWS...\n")
}

startTime = Sys.time()

cat("Loading preliminary data...")
firstDataYear = as.numeric(swsContext.computationParams$firstDataYear)
firstYear = as.numeric(swsContext.computationParams$firstYear)
lastYear = as.numeric(swsContext.computationParams$lastYear)
stopifnot(firstDataYear <= firstYear)
stopifnot(firstYear <= lastYear)
stopifnot(firstYear <= lastYear)

toProcess = fread(paste0(R_SWS_SHARE_PATH,
                         "/browningj/production/slaughtered_synchronized.csv"),
                  colClasses = "character")
toProcess[, c("Item Name", "Child Item Name") := NULL]
# Filter to just meats => CPC code like 2111* or 2112* (21111.01, 21112, ...)
toProcess = toProcess[grepl("^211(1|2|7).*", measuredItemChildCPC), ]

## Read the data.  The years and countries provided in the session are used, and
## the commodities in the session are somewhat considered. For example, if 02111
## (Cattle) is in the session, then the session will be expanded to also include
## 21111.01 (meat of cattle, fresh or chilled), 21151 (edible offal of cattle,
## fresh, chilled or frozen), 21512 (cattle fat, unrendered), and 02951.01 (raw
## hides and skins of cattle).  The measured element dimension of the session is
## simply ignored.

## Expand the session to include missing meats
key = swsContext.datasets[[1]]
rowsIncluded = toProcess[, measuredItemParentCPC %in% key@dimensions$measuredItemCPC@keys |
                           measuredItemChildCPC %in% key@dimensions$measuredItemCPC@keys]
requiredMeats = toProcess[rowsIncluded, c(measuredItemParentCPC, measuredItemChildCPC)]
key@dimensions[[itemVar]]@keys = requiredMeats
if(length(key@dimensions$measuredItemCPC@keys) == 0){
    stop("No meat/animal commodities are in the session, and thus this ",
         "module has nothing to do.")
}

## Update the measuredElements
key@dimensions[[elementVar]]@keys =
    unique(toProcess[rowsIncluded, c(measuredElementParent, measuredElementChild)])

## Adjust the years based on the passed information:
key@dimensions[[yearVar]]@keys =
    as.character(firstDataYear:lastYear)

## Include all countries, since all data is required for the imputation
key@dimensions[[areaVar]]@keys =
    GetCodeList("agriculture", "aproduction", "geographicAreaM49")[type == "country", code]

# Execute the get data call.
cat("Pulling the data")
data = GetData(key = key)




## Step 1. Transfer down the slaughtered animal numbers from the animal (parent)
## commodity to the meat (child) commodity.

# Remove missing values, as we don't want to copy those.
parentData = data[!flagObservationStatus == "M" &
                  measuredItemCPC %in% toProcess$measuredItemParentCPC &
                  timePointYears <= lastYear & timePointYears >= firstDataYear, ]
setnames(parentData, c(itemVar, elementVar),
         c("measuredItemParentCPC", "measuredElementParent"))
childData = merge(parentData, toProcess,
                  by = c("measuredItemParentCPC", "measuredElementParent"))
childData[, c("measuredItemParentCPC", "measuredElementParent") := NULL]
setnames(childData, c("measuredItemChildCPC", "measuredElementChild"),
         c(itemVar, elementVar))
data = merge(data, childData, all = TRUE, suffixes = c("", ".new"),
             by = c(areaVar, itemVar, elementVar, yearVar))
data[!is.na(Value.new), c("Value", "flagObservationStatus", "flagMethod") :=
         list(Value.new, flagObservationStatus.new, flagMethod.new)]
data[, c("Value.new", "flagObservationStatus.new", "flagMethod.new") := NULL]
SaveData("agriculture", "aproduction", data = data)



## Step 2. Impute the meat data (production/animals slaughtered/carcass weight)
## following the logic from the production imputation module.

uniqueItem = key@dimensions$measuredItemCPC@keys
# Just impute the meat elements
uniqueItem = uniqueItem[uniqueItem %in% toProcess$measuredItemChildCPC]
uniqueItem = as.character(uniqueItem)

result = NULL
for(iter in 1:length(uniqueItem)){
    singleItem = uniqueItem[iter]
    subKey = key
    subKey@dimensions$measuredItemCPC@keys = singleItem
    print(paste0("Imputation for item: ", singleItem))
    
    impute = try({
        cat("Reading in the data...\n")
        datasets = getProductionData(subKey)
        # Ignore indigenous/biological:
        datasets$formulaTuples = datasets$formulaTuples[nchar(input) == 4, ]
        
        for(i in 1:nrow(datasets$formulaTuples)){
            cat("Processing pair", i, "of", nrow(datasets$formulaTuples),
                "element triples.\n")
            datasets = cleanData(datasets, i = i, maxYear = 2014)
            
            ## Setup for the imputation
            processingParams = defaultProcessingParameters(
                productionValue = datasets$formulaTuples[, output][i],
                yieldValue = datasets$formulaTuples[, productivity][i],
                areaHarvestedValue = datasets$formulaTuples[, input][i])
            p = getImputationParameters(datasets, i = i)
            yieldParams = p$yieldParams
            # Delete this next line after 18/02/2016
            yieldParams$ensembleModels$defaultLogistic@model = defaultLogistic
            productionParams = p$productionParams
            # Delete these next line(s) after 18/02/2016
            productionParams$ensembleModels$defaultLogistic@model = defaultLogistic

            ## Original design was to remove old estimated imputation values. 
            ## Salar then requested to keep them, and then we decided to remove
            ## them again.
            processingParams$removePriorImputation = TRUE
            computeYield(datasets$query, newMethodFlag = "i",
                         processingParameters = processingParams,
                         unitConversion = datasets$formulaTuples$unitConversion)

            datasets$query[, countryCnt := .N, by = c(processingParams$byKey)]
            datasets$query = datasets$query[countryCnt > 1, ]
            datasets$query[, countryCnt := NULL]
            
            # Determine which observations shouldn't be imputed:
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
            ## Production must be zero if area harvested is 0.
            zeroProd = datasets$query[get(processingParams$productionValue) == 0 &
                                      get(processingParams$productionObservationFlag) != "M",
                                      c(processingParams$yearValue, productionParams$byKey),
                                      with = FALSE]
            zeroArea = datasets$query[get(processingParams$areaHarvestedValue) == 0 &
                                      get(processingParams$areaHarvestedObservationFlag) != "M",
                                      c(processingParams$yearValue, productionParams$byKey),
                                      with = FALSE]
            forcedZero = unique(rbind(assumedZero, zeroProd, zeroArea))
            
            out = imputeProductionDomain(datasets$query, processingParameters = processingParams,
                    yieldImputationParameters = yieldParams,
                    productionImputationParameters = productionParams,
                    unitConversion = datasets$formulaTuples$unitConversion)
            
            ## Remove the observations we don't want to impute on
            ## Use keys so we can do an anti-join
            setkeyv(forcedZero, colnames(forcedZero))
            setkeyv(out, colnames(forcedZero))
            out = out[!forcedZero, ]
            timeFilter = data.table(timePointYears = firstYear:lastYear,
                                    key = "timePointYears")
            setkeyv(out, "timePointYears")
            out = out[timeFilter, ]
            out[, ensembleVariance := NULL]
        } # close item type for loop
        out
    }) # close try block
    if(inherits(impute, "try-error")){
        print("Imputation Module Failed")
    } else {
        print("Imputation Module Executed Successfully")
        SaveData("agriculture", "aproduction", data = impute, normalized = FALSE)
        ## Just need to return the numbers slaughtered code:
        impute[, paste0(c("Value", "flagObservationStatus", "flagMethod"),
                        "_measuredElement_", datasets$formulaTuples$productivity) := NULL]
        impute[, paste0(c("Value", "flagObservationStatus", "flagMethod"),
                        "_measuredElement_", datasets$formulaTuples$output) := NULL]
        impute[, measuredElement := datasets$formulaTuples$input]
        setnames(impute, colnames(impute),
                 gsub("_measuredElement_.*", "", colnames(impute)))
        result = rbind(result, impute)
    }
}




## Step 3. Copy the slaughtered animal numbers in meat back to the animal commodity.
childData = copy(result)
setnames(childData, c(itemVar, elementVar),
         c("measuredItemChildCPC", "measuredElementChild"))
parentData = merge(childData, toProcess,
                  by = c("measuredItemChildCPC", "measuredElementChild"))
parentData[, c("measuredItemChildCPC", "measuredElementChild") := NULL]
setnames(parentData, c("measuredItemParentCPC", "measuredElementParent"),
         c(itemVar, elementVar))
parentData[, timePointYears := as.character(timePointYears)]
## Only need to keep the updated data
data = merge(data, parentData, all.y = TRUE, suffixes = c("", ".new"),
             by = c(areaVar, itemVar, elementVar, yearVar))
data = data[is.na(Value) & !is.na(Value.new), ]
data[, c("Value", "flagObservationStatus", "flagMethod") :=
         list(Value.new, flagObservationStatus.new, flagMethod.new)]
data[, c("Value.new", "flagObservationStatus.new", "flagMethod.new") := NULL]



## Step 4. Save all three variables for meat (production/animals
## slaughterd/carcass weight) and the animals slaughtered for the animal.
## 
## Note: the first saving has been done, we just need to now save the data under
## the animal element.

saveResult = SaveData(domain = "agriculture", dataset = "aproduction",
                      data = data)

message = paste("Module completed successfully.")
cat(message)

message
