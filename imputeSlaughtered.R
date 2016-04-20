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

minObsForEst = 5
yearsModeled = 20
## server is only used for debug sessions:
## server = "Prod"
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
            ## token = "f8646896-2ed2-4e88-9cd2-9db6d735991f"
            ## New Token for all the meats
            token = "4fe1052b-bfb0-45fa-b9ec-2dda5a1b9421"
        )
    }
    sapply(files, source)
} else {
    cat("Working on SWS...\n")
}

## Just testing 1 item
## swsContext.datasets[[1]]@dimensions[[measuredItemCPC]]@keys = "21111.01" ## meat of cattle
## swsContext.datasets[[1]]@dimensions[[measuredItemCPC]]@keys = "21113.01" ## meat of pig

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
selectedMeatTable = toProcess[grepl("^211(1|2|7).*", measuredItemChildCPC), ]

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
                                    selectedMeatTable = selectedMeatTable)

## Execute the get data call.
cat("Pulling the data...\n")

step1Data = 
    newKey %>%
    GetData(key = .) %>%
    preProcessing(data = .) %>%
    ## TODO (Michael): Should add preProcessing here
    transferAnimalNumber(data = ., selectedMeatTable)

## Module test and save the transfered data back
step1Data %>%
    postProcessing(data = .) %>%
    checkProtectedData(dataToBeSaved = .) %>%
    SaveData("agriculture", "aproduction", data = .)



## Step 2. Impute the meat data (production/animals
##         slaughtered/carcass weight) following the logic from the
##         production imputation module.
##
## NOTE (Michael): The imputed data for meat triplet is also saved
##                 back in this step.

selectedMeatCode =
    getSessionMeatSelection(key = newKey,
                            selectedMeatTable = selectedMeatTable)

result = NULL
successCount = 0
failCount = 0
for(iter in 1:length(selectedMeatCode)){
    currentMeat = selectedMeatCode[iter]
    subKey = newKey
    subKey@dimensions$measuredItemCPC@keys = currentMeat
    print(paste0("Imputation for item: ", currentMeat))

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


if(!is.null(result)){
    saveResult =
        ## Step 3. Copy the slaughtered animal numbers in meat back to the
        ##         animal commodity.
        transferSlaughteredNumber(step1Data, result) %>%
        ## Post process the data 
        postProcessing(data = .) %>%
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
