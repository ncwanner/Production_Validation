library(faosws)
library(data.table)

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
    
    ## Define directories
    if(Sys.info()[7] == "josh"){
        apiDirectory = "~/Documents/Github/faoswsProduction/R/"
        R_SWS_SHARE_PATH = "/media/hqlprsws1_qa/"
        ## R_SWS_SHARE_PATH = "/media/hqlprsws2_prod"
    } else if(Sys.info()[7] == "rockc_000"){
        apiDirectory = "~/Github/faoswsProduction/R/"
        stop("Can't connect to share drives!")
    }

    ## Get SWS Parameters
    SetClientFiles(dir = "~/R certificate files/QA")
    GetTestEnvironment(
        ## baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
        ## token = "7b588793-8c9a-4732-b967-b941b396ce4d"
        baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
        token = "8c65d175-6f8a-4156-b90c-860581e302be"
    )

    ## Source local scripts for this local test
    for(file in dir(apiDirectory, full.names = T))
        source(file)
}

startYear = as.numeric(swsContext.computationParams$startYear)
endYear = as.numeric(swsContext.computationParams$endYear)
countryM49 = swsContext.datasets[[1]]@dimensions$geographicAreaM49@keys

stopifnot(startYear <= endYear)

successCount = 0
failureCount = 0

## Loop through the items and save production/yield data:
for(singleItem in swsContext.datasets[[1]]@dimensions$measuredItemCPC@keys){
    
    formulaTuples = getYieldFormula(singleItem)
        
    for(i in 1:nrow(formulaTuples)){
        ## Verify that the years requested by the user (in swsContext.params) are
        ## possible based on the constructed model.
        loadData = try({
            load(paste0(R_SWS_SHARE_PATH, "/browningj/production/prodModel_",
                        singleItem, "_", i, ".RData"))
        })
        if(is(loadData, "try-error")){
            failureCount = failureCount + 1
        } else {
            successCount = successCount + 1
        
            obsStartYear = min(modelProduction$fit$timePointYears)
            obsEndYear = max(modelProduction$fit$timePointYears)
            if(obsStartYear > startYear){
                stop(paste0("Model has been constructed on data that only exists ",
                            "after the supplied start year.  Start Year must be at ",
                            "least ", obsStartYear))
            }
            if(obsEndYear < endYear){
                stop(paste0("Model has been constructed on data that only exists ",
                            "before the supplied year.  End Year must be at ",
                            "most ", obsEndYear))
            }
            
            ## Restructure modelProduction for saving
            modelProduction = modelProduction$fit[, variance := NULL]
            modelProduction = modelProduction[timePointYears <= endYear &
                                              timePointYears >= startYear &
                                              geographicAreaM49 %in% countryM49, ]
            modelProduction[, measuredElement := formulaTuples[i, output]]
            modelProduction[, Value := fit]
            modelProduction[, fit := NULL]
            modelProduction[, flagObservationStatus := "I"]
            modelProduction[, flagMethod := "e"]
            
            modelYield = modelYield$fit[, variance := NULL]
            modelYield = modelYield[timePointYears <= endYear &
                                    timePointYears >= startYear &
                                    geographicAreaM49 %in% countryM49, ]
            modelYield[, measuredElement := formulaTuples[i, productivity]]
            modelYield[, Value := fit]
            modelYield[, fit := NULL]
            modelYield[, flagObservationStatus := "I"]
            modelYield[, flagMethod := "e"]
            
            dataToSave = rbind(modelYield, modelProduction)
            dataToSave = dataToSave[!is.na(Value), ]
            SaveData(domain = "agriculture", dataset = "agriculture",
                     data = dataToSave)
        }
    }
}

paste0("Module completed with ", successCount, " successes out of ",
       successCount + failureCount, " executions.")
