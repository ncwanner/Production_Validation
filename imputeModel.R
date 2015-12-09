library(faosws)
library(faoswsUtil)
library(data.table)

## Setting up variables
areaVar = "geographicAreaM49"
yearVar = "timePointYears"
itemVar = "measuredItemCPC"
elementVar = "measuredElement"

defaultStartYear = 2010
defaultEndYear = 2014

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
        token = "b55f4ae3-5a0c-4514-b89e-d040112bf25e"
    )

    ## Source local scripts for this local test
    for(file in dir(apiDirectory, full.names = T))
        source(file)
}

if(is.null(swsContext.computationParams$startYear)){
    startYear = defaultStartYear
} else {
    startYear = as.numeric(swsContext.computationParams$startYear)
}
if(is.null(swsContext.computationParams$endYear)){
    endYear = defaultEndYear
} else {
    endYear = as.numeric(swsContext.computationParams$endYear)
}
countryM49 = swsContext.datasets[[1]]@dimensions$geographicAreaM49@keys

stopifnot(startYear <= endYear)

successCount = 0
failureCount = 0

## Loop through the items and save production/yield data:
for(singleItem in swsContext.datasets[[1]]@dimensions$measuredItemCPC@keys){
    
    formulaTuples = try(getYieldFormula(singleItem))
    if(is(formulaTuples, "try-error")){
        failureCount = failureCount + 1
        next
    }
        
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
            if(any(!startYear:endYear %in% years)){
                stop(paste0("Model has been constructed on data that does not ",
                            "contain the desired imputation range!  Please ",
                            "update start and end year to fall in this range: ",
                            paste(years, collapse = ", ")))
            }
            
            ## Restructure modelProduction for saving
            if(!is.null(modelProduction)){
                ## If not NULL, extract needed info.  Otherwise, if NULL (i.e. 
                ## model failed) don't do anything (as saving a dataset with
                ## NULL shouldn't cause problems).
                modelProduction = modelProduction$fit[, variance := NULL]
                modelProduction = modelProduction[timePointYears <= endYear &
                                                  timePointYears >= startYear &
                                                  geographicAreaM49 %in% countryM49, ]
                modelProduction[, measuredElement := formulaTuples[i, output]]
                modelProduction[, Value := fit]
                modelProduction[, Value := sapply(Value, roundResults)]
                modelProduction[, fit := NULL]
                modelProduction[, flagObservationStatus := "I"]
                modelProduction[, flagMethod := "e"]
            }
            
            if(!is.null(modelYield)){
                ## See comment for modelProduction
                modelYield = modelYield$fit[, variance := NULL]
                modelYield = modelYield[timePointYears <= endYear &
                                        timePointYears >= startYear &
                                        geographicAreaM49 %in% countryM49, ]
                modelYield[, measuredElement := formulaTuples[i, productivity]]
                modelYield[, Value := fit]
                modelYield[, fit := NULL]
                modelYield[, flagObservationStatus := "I"]
                modelYield[, flagMethod := "e"]
            }
            
            dataToSave = rbind(modelYield, modelProduction)
            ## HACK: Update China and Pacific
            warning("Hack below!  Remove once the geographicAreaM49 dimension is fixed!")
            dataToSave = dataToSave[!geographicAreaM49 %in% c("1249", "156", "582"), ]
            dataToSave = dataToSave[!is.na(Value), ]
            if((!is.null(dataToSave)) && nrow(dataToSave) > 0){
                saveProductionData(data = dataToSave,
                        areaHarvestedCode = formulaTuples[i, input],
                        yieldCode = formulaTuples[i, productivity],
                        productionCode = formulaTuples[i, output],
                        normalized = TRUE)
            }
        }
    }
}

paste0("Module completed with ", successCount, "/",
       successCount + failureCount, " commodities imputed.")
