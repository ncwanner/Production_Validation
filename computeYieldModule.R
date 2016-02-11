########################################################################
## Title: Yield Computation Module for SWS
## Author: Josh Browning
########################################################################

library(data.table)
library(faosws)
library(faoswsFlag)
library(faoswsUtil)

areaVar = "geographicAreaM49"
yearVar = "timePointYears"
itemVar = "measuredItemCPC"
elementVar = "measuredElement"
yieldElements = c(31, 41, 51)

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
DEBUG_MODE = Sys.getenv("R_DEBUG_MODE")

if(!exists("DEBUG_MODE") || DEBUG_MODE == ""){
    cat("Not on server, so setting up environment...\n")

    if(Sys.info()[7] == "josh"){ # Josh work
        files = dir("~/Documents/Github/faoswsProduction/R/",
                    full.names = TRUE)
        SetClientFiles("~/R certificate files/Production/")
    } else if(Sys.info()[7] == "rockc_000"){ # Josh personal
        files = dir("~/Github/faoswsProduction/R/", full.names = TRUE)
    } else {
        stop("Add your github directory here!")
    }
        
    ## Get SWS Parameters
    GetTestEnvironment(
        baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
        token = "916b73ad-2ef5-4141-b1c4-769c73247edd"
        # baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
        # token = "feed1154-6590-4ea9-9e6e-2c81f960d0dd"
    )
    sapply(files, source)
} else {
    cat("Working on SWS...\n")
}

startTime = Sys.time()

## Variable to determine if all yield data should be computed (across entire
## database) or just local session.
allData = !is.null(swsContext.computationParams$updateAll) &&
    swsContext.computationParams$updateAll == "all"

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

    key = dataContext
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
if(allData){
    swsContext.datasets[[1]]@dimensions[[areaVar]]@keys =
        GetCodeList(domain = "agriculture", dataset = "aproduction",
                    dimension = areaVar)[type == "country", code]
    swsContext.datasets[[1]]@dimensions[[elementVar]]@keys =
        GetCodeList(domain = "agriculture", dataset = "aproduction",
                    dimension = elementVar)[type %in% yieldElements, code]
    swsContext.datasets[[1]]@dimensions[[itemVar]]@keys =
        GetCodeList(domain = "agriculture", dataset = "aproduction",
                    dimension = itemVar)[, code]
    swsContext.datasets[[1]]@dimensions[[yearVar]]@keys =
        GetCodeList(domain = "agriculture", dataset = "aproduction",
                    dimension = yearVar)[description != "wildcard", code]
}

## Pull data
## 
## Create "yearList" to loop over.  If this module is being run on a session, we
## shouldn't loop over year.  In that case, yearList will just be an list of 
## length one, and so the loop will be ran once.  Otherwise, yearList will be a
## list of all the individual years, and so the loop will run over each year
## individually.
if(allData){
    yearList = as.list(swsContext.datasets[[1]]@dimensions$timePointYears@keys)
} else {
    yearList = list(swsContext.datasets[[1]]@dimensions$timePointYears@keys)
}
queryResult = c()
## FOR LOOP!  This isn't going to cause much of a performance issue since we're 
## not looping through a ton of items (only at most 50 or 60) and we're not 
## using those items to access different chunks of a data.frame.
for(years in yearList){
    swsContext.datasets[[1]]@dimensions$timePointYears@keys = years
    formulaTuples =
        getYieldFormula(slot(slot(swsContext.datasets[[1]],
                                  "dimensions")$measuredItemCPC, "keys"),
                        warn = TRUE)
    uniqueLevels = unique(formulaTuples[, list(input, productivity, output,
                                               unitConversion)])
    ## FOR LOOP!  This could be vectorized, but it would require some kind of
    ## "mapply"-ish vectorization.  It's not worth the confusion in this case,
    ## especially since there shouldn't be performance problems with this
    ## appraoch.
    for(i in 1:nrow(uniqueLevels)){
        test = try({
            filter = uniqueLevels[i, ]
            
            ## Get all the CPC codes we need by merging the specific
            ## production/output/input codes with the dataset.
            currentCPC = merge(formulaTuples, filter,
                               by = c("input", "productivity", "output",
                                      "unitConversion"))[, measuredItemCPC]
            
            ## Filter the context to just the revelant item/element keys
            swsContext.datasets[[1]]@dimensions$measuredElement@keys =
                as.character(filter[, list(input, output, productivity)])
            swsContext.datasets[[1]]@dimensions$measuredItemCPC@keys =
                currentCPC
            data = getYieldData(swsContext.datasets[[1]])
            
            processingParams = defaultProcessingParameters(
                productionValue = filter[, output],
                yieldValue = filter[, productivity],
                areaHarvestedValue = filter[, input])
            
            computeYield(data = data$query, processingParameters = processingParams,
                         unitConversion = filter$unitConversion)
            balanceProduction(data = data$query, processingParameters = processingParams,
                         unitConversion = filter$unitConversion)
            balanceAreaHarvested(data = data$query, processingParameters = processingParams,
                         unitConversion = filter$unitConversion)
            if(nrow(data$query) >= 1){
                saveProductionData(data$query, areaHarvestedCode = filter$input,
                                   yieldCode = filter$productivity,
                                   productionCode = filter$output, normalized = FALSE)
            }
        })
        queryResult = c(queryResult, is(test, "try-error"))
    }
}

paste("Module completed with", sum(queryResult), "errors in",
      round(difftime(Sys.time(), startTime, units = "min"), 2), "minutes.")
