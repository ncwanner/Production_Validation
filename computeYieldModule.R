########################################################################
## Title: Yield Computation Module for SWS
## Date:2014
## 2015-06-30
## Author: Josh Browning
########################################################################

library(data.table)
library(faosws)
library(faoswsFlag)
library(faoswsUtil)

## Setting up variables
areaVar = "geographicAreaM49"
yearVar = "timePointYears"
itemVar = "measuredItemCPC"
elementVar = "measuredElement"

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
DEBUG_MODE = Sys.getenv("R_DEBUG_MODE")

if(!exists("DEBUG_MODE") || DEBUG_MODE == ""){
    cat("Not on server, so setting up environment...\n")
    
    ## Get SWS Parameters
    GetTestEnvironment(
        ## baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
        ## token = "e77abee7-9b0d-4557-8c6f-8968872ba7ca"
        baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
        token = "056d0a78-8a3e-486d-816a-0b25dc927cc1"
    )
    if(Sys.info()[7] == "josh"){
        files = dir("~/Documents/Github/faoswsProduction/R/",
                    full.names = TRUE)
        files = c(files, dir("~/Documents/Github/faoswsProduction/RfaoswsUtilSource/",
                    full.names = TRUE))
    } else {
        stop("Add your github directory here!")
    }
    sapply(files, source)
} else {
    cat("Working on SWS...\n")
}

    
## Function for obtaining the data and meta data.
getYieldData = function(dataContext){
    ## Setups
    formulaTuples =
        getYieldFormula(slot(slot(dataContext,
                                  "dimensions")$measuredItemCPC, "keys"))
    ## setting the prefix, also should be accessed by the API
    prefixTuples =
        data.table(
            valuePrefix = "Value_measuredElement_",
            flagObsPrefix = "flagObservationStatus_measuredElement_",
            flagMethodPrefix = "flagMethod_measuredElement_"
            )

    ## Pivot to vectorize yield computation
    newPivot = c(
        Pivoting(code = areaVar, ascending = TRUE),
        Pivoting(code = itemVar, ascending = TRUE),
        Pivoting(code = yearVar, ascending = FALSE),
        Pivoting(code = elementVar, ascending = TRUE)
        )
    slot(slot(dataContext, "dimensions")$measuredElement, "keys") =
        unique(unlist(formulaTuples[, list(input,
                                           productivity, output)]))

    ## Query the data
    query = GetData(
        key = dataContext,
        flags = TRUE,
        normalized = FALSE,
        pivoting = newPivot
        )
    
    list(query = query,
         formulaTuples = formulaTuples,
         prefixTuples = prefixTuples)
}

## If all yields should be updated, extend the key
if(!is.null(swsContext.computationParams$updateAll) &&
   swsContext.computationParams$updateAll == "all"){
    swsContext.datasets[[1]]@dimensions$geographicAreaM49@key =
        GetCodeList(domain = "agriculture", dataset = "agriculture",
                    dimension = "geographicAreaM49")[type == "country", code]
    swsContext.datasets[[1]]@dimensions$measuredElement@key =
        GetCodeList(domain = "agriculture", dataset = "agriculture",
                    dimension = "measuredElement")[type %in% c(31, 41, 51), code]
    swsContext.datasets[[1]]@dimensions$measuredItemCPC@key =
        GetCodeList(domain = "agriculture", dataset = "agriculture",
                    dimension = "measuredItemCPC")[, code]
    swsContext.datasets[[1]]@dimensions$timePointYears@key =
        GetCodeList(domain = "agriculture", dataset = "agriculture",
                    dimension = "timePointYears")[description != "wildcard", code]
}

## Pull data
data = getYieldData(swsContext.datasets[[1]])
uniqueLevels = unique(data$formulaTuples[, list(input, productivity, output,
                                                unitConversion)])
queryResult = c()
for(i in 1:nrow(uniqueLevels)){
    test = try({
        filter = uniqueLevels[i, ]
        ## Get all the CPC codes we need by merging the specific
        ## production/output/input codes with the dataset.
        currentCPC = merge(data$formulaTuples, uniqueLevels,
                           by = c("input", "productivity", "output",
                                  "unitConversion"))[, measuredItemCPC]
        ## Note: we have to subset the data here (rather than in the function
        ## call) because the data isn't returned (but rather is just updated as
        ## a data.table).
        subData = data$query[measuredItemCPC %in% currentCPC, ]
        computeYield(data = subData,
## CHANGE THIS!!!
##        faoswsUtil::computeYield(data = subData,
            productionValue = paste0("Value_measuredElement_", filter[, output]),
            productionObservationFlag = paste0("flagObservationStatus_measuredElement_", filter[, output]),
            areaHarvestedValue = paste0("Value_measuredElement_", filter[, input]),
            areaHarvestedObservationFlag = paste0("flagObservationStatus_measuredElement_", filter[, input]),
            yieldValue = paste0("Value_measuredElement_", filter[, productivity]),
            yieldObservationFlag = paste0("flagObservationStatus_measuredElement_", filter[, productivity]),
            yieldMethodFlag = paste0("flagMethod_measuredElement_", filter[, productivity]),
            unitConversion = filter[, unitConversion])
        saveProductionData(subData, areaHarvestedCode = filter$input,
                           yieldCode = filter$productivity,
                           productionCode = filter$output)
    })
    queryResult = c(queryResult, is(test, "try-error"))
}

paste("Module completed with", sum(queryResult), "errors.")
