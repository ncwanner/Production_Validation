###############################################################################
## Title: Stock Synchronization Module for SWS
##
## Author: Josh Browning, editted by Michael C. J. Kao
##
## This module is used to synchronize various cells in the production database.
## For example, Cattle (02111) has element 5315 (animals slaughtered) and this
## should be the same as 5320 (animals slaughtered) for commodities 21111.01
## (meat of cattle, fresh or chilled), 21151 (edible offal of cattle, fresh,
## chilled or frozen), 21512 (cattle fat, unrendered), and 02951.01 (raw hides
## and skins of cattle).
##
## NOTE (Michael): This is not really a synchronise module, it merely
##                 transfer from parents to children.
##
## NOTE (Michael): According to previous comments, this is the only
##                 module where official figure and semi-official data
##                 in the child commodity can be over-written. This is
##                 due to the consideration that these official
##                 figures are old.
###############################################################################

cat("Beginning slaughtered synchronization script...\n")
suppressMessages({
    library(faosws)
    library(faoswsUtil)
    library(faoswsProduction)
    library(dplyr)
    library(magrittr)
})

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

## This return FALSE if on the Statistical Working System
if(CheckDebug()){

    library(faoswsModules)
    SETTINGS = ReadSettings("sws.yml")

    ## If you're not on the system, your settings will overwrite any others
    R_SWS_SHARE_PATH = SETTINGS[["share"]]

    ## Define where your certificates are stored
    SetClientFiles(SETTINGS[["certdir"]])

    ## Get session information from SWS. Token must be obtained from web interface
    GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                       token = SETTINGS[["token"]])

}
startTime = Sys.time()

cat("Loading preliminary data and create expanded Datakey...\n")
## Read the data.  The years and countries provided in the session are
## used, and the commodities in the session are somewhat
## considered. For example, if 02111 (Cattle) is in the session, then
## the session will be expanded to also include 21111.01 (meat of
## cattle, fresh or chilled), 21151 (edible offal of cattle, fresh,
## chilled or frozen), 21512 (cattle fat, unrendered), and 02951.01
## (raw hides and skins of cattle).  The measured element dimension of
## the session is simply ignored.
##
## NOTE (Michael): It seems there is no point of pulling the data from
##                 the children, as the mapping comes from the
##                 commodity tree and new values are calculated.

selectedMeatTable =
    getAnimalMeatMapping(R_SWS_SHARE_PATH = R_SWS_SHARE_PATH,
                         onlyMeatChildren = FALSE) %>%
    subsetAnimalMeatMapping(animalMeatMapping = .,
                            context = swsContext.datasets[[1]])
expandedMeatKey =
    selectedMeatTable %>%
    expandMeatSessionSelection(oldKey = swsContext.datasets[[1]],
                               selectedMeatTable = .)

## Execute the get data call.
cat(length(expandedMeatKey@dimensions$measuredItemCPC@keys),
    "commodities selected to be synchronised \n")
cat("Pulling the data ... \n")

newParentData =
    GetData(key = expandedMeatKey) %>%
    fillRecord(data = .) %>%
    checkFlagValidity(data = .) %>%
    ## TODO (Michael): Add in the check for production input, however, have to
    ##                 account for multiple formulas
    ## checkProductionInputs(data = .,
    ##                       processingParam = processingParams) %>%
    preProcessing(data = .) %>%
    ## Remove missing values, as we don't want to copy those.
    removeMissingEntry(data = .) %>%
    selectParentData(data = ., selectedMeatTable = selectedMeatTable)

cat("Pulling the commodity trees ...\n")

newCommodityTree =
    newParentData %$%
    getCommodityTree(geographicAreaM49 = as.character(unique(.$geographicAreaM49)),
                     timePointYears = as.character(unique(.$timePointYears))) %>%
    subset(., share > 0) %>%
    setnames(.,
             old = c("measuredItemParentCPC", "timePointYearsSP"),
             new = c("measuredItemCPC", "timePointYears"))

cat("Transfere the animal number to all the children commodities ... \n")
transferedData =
    newCommodityTree %>%
    mutate(timePointYears = as.numeric(timePointYears)) %>%
    transferParentToChild(commodityTree = .,
                          parentData = newParentData,
                          selectedMeatTable) %>%
    postProcessing(data = .)



cat("Testing module and saving data back ...\n")

animalNumberDB =
    getAllAnimalNumber(selectedMeatTable)

saveResult =
    transferedData %>%
    ## Check whether the values are synced
    checkSlaughteredSynced(commodityTree = newCommodityTree,
                           animalNumbers = animalNumberDB,
                           slaughteredNumbers = .) %>%
    .$slaughteredNumbers %>%
    ## NOTE (Michael): This section has been commented out, since the check is
    ##                 not required as mentioned in the comment in the
    ##                 beginning. Official and semi-official figures can be
    ##                 over-written.
    ## filter(x = ., flagMethod %in% c("i", "t", "e", "n", "u")) %>%
    ## checkProtectedData(dataToBeSaved = .) %>%
    SaveData(domain = swsContext.datasets[[1]]@domain,
             dataset = swsContext.datasets[[1]]@dataset,
             data = .)

result = paste("Module completed with", saveResult$inserted + saveResult$appended,
               "observations updated and", saveResult$discarded, "problems.\n")
cat(result)

result
