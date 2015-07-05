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
PROCESSING_CHUNKS = 10

if(!exists("DEBUG_MODE") || DEBUG_MODE == ""){
    cat("Not on server, so setting up environment...\n")
    
    ## Get SWS Parameters
    GetTestEnvironment(
        ## baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
        ## token = "e77abee7-9b0d-4557-8c6f-8968872ba7ca"
        baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
        token = "8f2ac965-73ad-4d6d-906c-a7ac49f21fd2"
    )
    if(Sys.info()[7] == "josh"){ # Josh work
        files = dir("~/Documents/Github/faoswsProduction/R/",
                    full.names = TRUE)
#         files = c(files, dir("~/Documents/Github/faoswsProduction/otherFuncs/faoswsUtil- install on server/",
#                     full.names = TRUE))
#         files = c(files, dir("~/Documents/Github/faoswsProduction/otherFuncs/faoswsImputation- install on server/",
#                     full.names = TRUE))
    } else if(Sys.info()[7] == "rockc_000"){ # Josh personal
        files = dir("~/Github/faoswsProduction/R/", full.names = TRUE)
#         files = c(files, dir("~/Github/faoswsProduction/otherFuncs/faoswsUtil- install on server/"
#                     full.names = TRUE))
#         files = c(files, dir("~/Github/faoswsProduction/otherFuncs/faoswsImputation- install on server/",
#                     full.names = TRUE))
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

    key = swsContext.datasets[[1]]
    if (exists("swsContext.modifiedCells")){
        print("Checking only modified data.")
        nmodded = nrow(swsContext.modifiedCells)
    
        if (nmodded > 0){
            data2process = TRUE
            for(cname in colnames(swsContext.modifiedCells)){
                key@dimensions[[cname]]@keys = as.character(unique(
                    swsContext.modifiedCells[, cname, with = FALSE]))
            }
        }
    } else {# running in non-interactive mode, so get all the session data
        print("Reading all data.")
        data2process = TRUE
    }
     
    ## Pivot to vectorize yield computation
    newPivot = c(
        Pivoting(code = areaVar, ascending = TRUE),
        Pivoting(code = itemVar, ascending = TRUE),
        Pivoting(code = yearVar, ascending = FALSE),
        Pivoting(code = elementVar, ascending = TRUE)
        )
    ## Just extract yield-related elements
    slot(slot(key, "dimensions")$measuredElement, "keys") =
        unique(unlist(formulaTuples[, list(input,
                                           productivity, output)]))

    if (data2process == TRUE){
        # Execute the get data call. 
        query = GetData(
            key = key,
            flags = TRUE,
            normalized = FALSE,
            pivoting = newPivot
            )
    }
   ## Query the data
    
    list(query = query,
         formulaTuples = formulaTuples,
         prefixTuples = prefixTuples)
}

## If all yields should be updated, extend the key
if(!is.null(swsContext.computationParams$updateAll) &&
   swsContext.computationParams$updateAll == "all"){
    swsContext.datasets[[1]]@dimensions$geographicAreaM49@keys =
        GetCodeList(domain = "agriculture", dataset = "agriculture",
                    dimension = "geographicAreaM49")[type == "country", code]
    swsContext.datasets[[1]]@dimensions$measuredElement@keys =
        GetCodeList(domain = "agriculture", dataset = "agriculture",
                    dimension = "measuredElement")[type %in% c(31, 41, 51), code]
    swsContext.datasets[[1]]@dimensions$measuredItemCPC@keys =
        GetCodeList(domain = "agriculture", dataset = "agriculture",
                    dimension = "measuredItemCPC")[, code]
    swsContext.datasets[[1]]@dimensions$timePointYears@keys =
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
        
        processingParams = defaultProcessingParameters(
            productionValue = filter[, output],
            yieldValue = filter[, productivity],
            areaHarvestedValue = filter[, input])
        
        computeYield(data = subData, processingParameters = processingParams,
                     unitConversion = filter$unitConversion)
        balanceProduction(data = subData, processingParameters = processingParams,
                     unitConversion = filter$unitConversion)
        balanceAreaHarvested(data = subData, processingParameters = processingParams,
                     unitConversion = filter$unitConversion)
        ## Use waitMode = "forget" to avoid server timeouts
        saveProductionData(subData, areaHarvestedCode = filter$input,
                           yieldCode = filter$productivity,
                           productionCode = filter$output, waitMode = "forget")
    })
    queryResult = c(queryResult, is(test, "try-error"))
}

paste("Module completed with", sum(queryResult), "errors.")
