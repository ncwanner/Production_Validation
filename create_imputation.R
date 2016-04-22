## load the library
library(faosws)
library(faoswsUtil)
library(faoswsFlag)
library(faoswsImputation)
library(data.table)
library(splines)
library(lme4)
library(magrittr)

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
        SetClientFiles(dir = ifelse(server == "Prod",
                                    "~/R certificate files/Production/",
                                    "~/R certificate files/QA/"))
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
    } else if(Sys.info()[7] == "kao"){
        apiDirectory = "R/"
        R_SWS_SHARE_PATH = ifelse(server == "Prod",
                                  "/media/sws_prod_shared_drive/",
                                  "/media/sws_qa_shared_drive/")
        SetClientFiles(dir = ifelse(server == "Prod", "~/.R/prod/", "~/.R/qa/"))
    }


    ## Get SWS Parameters

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


lastYear = as.numeric(swsContext.computationParams$lastYear)
firstYear = lastYear - yearsModeled + 1 # Rolling time range of yearsModeled years
years = firstYear:lastYear

newKey = getMainKey(years = years)

## Just testing 1 commodity
newKey@dimensions[[itemVar]]@keys = "0111" # Wheat


selectedItemCode = newKey@dimensions[[itemVar]]@keys

result = NULL
successCount = 0
failCount = 0
for(iter in 1:length(selectedItemCode)){
    currentMeat = selectedItemCode[iter]
    subKey = newKey
    subKey@dimensions$measuredItemCPC@keys = currentMeat
    print(paste0("Imputation for item: ", currentMeat, " (",  iter, " out of ",
                 length(selectedItemCode),")"))

    imputed = imputeMeatTriplet(meatKey = subKey)


    if(inherits(imputed, "try-error")){
        message("Imputation Module Failed!")
        failCount = failCount + 1
    } else {
        successCount = successCount + 1
        ## New module test
        imputed %>%
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


        message("Imputation Module Executed Successfully!")
        ## Just need to return the numbers slaughtered code:
        ##
        ## TODO (Michael): Need to restructure the get formula
        formulaTuples =
            getYieldFormula(slot(slot(subKey,
                                      "dimensions")$measuredItemCPC, "keys"))
        formulaTuples = formulaTuples[nchar(input) == 4, ]
        slaughteredAnimal =
            getSlaughteredAnimal(data = imputed,
                                 formulaTuples = formulaTuples)
        result = rbind(result, slaughteredAnimal)
    }
}

message = paste("Module completed with", successCount,
                "commodities imputed out of", successCount + failCount)
message(message)

message
