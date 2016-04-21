###############################################################################
## Title: Stock Synchronization Module for SWS
## 
## Author: Josh Browning
## 
## This module is used to synchronize various cells in the production database. 
## For example, Cattle (02111) has element 5315 (animals slaughtered) and this
## should be the same as 5320 (animals slaughtered) for commodities 21111.01
## (meat of cattle, fresh or chilled), 21151 (edible offal of cattle, fresh,
## chilled or frozen), 21512 (cattle fat, unrendered), and 02951.01 (raw hides
## and skins of cattle).
##
## NOTE (Michael): Sync from parents to children.
###############################################################################

cat("Beginning slaughtered synchronization script...\n")

library(data.table)
library(faosws)
library(faoswsUtil)
library(magrittr)

areaVar = "geographicAreaM49"
yearVar = "timePointYears"
itemVar = "measuredItemCPC"
elementVar = "measuredElement"

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if(CheckDebug()){
    cat("Not on server, so setting up environment...\n")

    if(Sys.info()[7] == "josh"){ # Josh work
        files = dir("~/Documents/Github/faoswsProduction/R/",
                    full.names = TRUE)
        SetClientFiles("~/R certificate files/QA/")
        R_SWS_SHARE_PATH = "/media/hqlprsws1_qa/"
    } else if(Sys.info()['user'] == "mk"){ # Josh work
        files = dir("R/", full.names = TRUE)
        SetClientFiles("~/.R/qa/")
        R_SWS_SHARE_PATH = "/media/sws_qa_shared_drive/"
    } else {
        stop("Add your github directory here!")
    }
        
    ## Get SWS Parameters
    GetTestEnvironment(
        # baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
        # token = "916b73ad-2ef5-4141-b1c4-769c73247edd"
        baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
        token = "63479732-0657-4b50-8c8d-c41bef92841b"
    )
    sapply(files, source)
} else {
    cat("Working on SWS...\n")
}

startTime = Sys.time()

cat("Loading preliminary data and create expanded Datakey...\n")

## NOTE (Michael): It seems there is no point of pulling the data from
##                 the children, as the mapping comes from the
##                 commodity tree and new values are calculated.

selectedMeatTable =
    getAnimalMeatMapping(R_SWS_SHARE_PATH = R_SWS_SHARE_PATH,
                         onlyMeatChildren = FALSE) %>%
    subsetAnimalMeatMapping(animalMeatMapping = .,
                            context = swsContext.datasets[[1]])
key = 
    selectedMeatTable %>%
    expandMeatSessionSelection(oldKey = swsContext.datasets[[1]],
                               selectedMeatTable = .)


# Execute the get data call.
cat("Pulling the data ... \n")

newParentData = 
    GetData(key = key) %>%
    preProcessing(data = .) %>%
    removeMissingEntry(data = .) %>%
    selectParentData(data = ., selectedMeatTable = selectedMeatTable)

cat("Pulling the commodity trees ...\n")

newCommodityTree =
    newParentData %$%
    getCommodityTree(geographicAreaM49 = as.character(unique(.$geographicAreaM49)),
                     timePointYears = as.character(unique(.$timePointYears))) %>%
    subset(., share > 0) %>%
    setnames(., c("measuredItemParentCPC", "timePointYearsSP"),
             c("measuredItemCPC", "timePointYears"))

cat("Transfere the animal number to all the children commodities ... \n")
transferedData = 
    newCommodityTree %>%
    preProcessing(.) %>%
    transferParentToChild(commodityTree = .,
                          parentData = newParentData,
                          selectedMeatTable) %>%
    postProcessing(data = .)



cat("Testing module and saving data back ...\n")

sessionAnimalMeatMapping =
    getAnimalMeatMapping(R_SWS_SHARE_PATH = R_SWS_SHARE_PATH) %>%
    subsetAnimalMeatMapping(animalMeatMapping = .,
                            context = swsContext.datasets[[1]])


animalNumberDB =
    getAllAnimalNumber(sessionAnimalMeatMapping)

saveResult =
    transferedData %>%
    checkSlaughteredSynced(animalMeatMapping = sessionAnimalMeatMapping,
                           animalNumbers = animalNumberDB,
                           slaughteredNumbers = .) %>%
    .$slaughteredNumbers %>%
    checkProtectedData(dataToBeSaved = .) %>%
    SaveData(domain = swsContext.datasets[[1]]@domain,
             dataset = swsContext.datasets[[1]]@dataset,
             data = .)

result = paste("Module completed with", saveResult$inserted + saveResult$appended,
      "observations updated and", saveResult$discarded, "problems.\n")
cat(result)

result
