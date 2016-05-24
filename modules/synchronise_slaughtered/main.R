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


## Step 0. Initial set-up

message("Beginning impute slaughtered script...\n")
suppressMessages({
    library(faosws)
    library(faoswsFlag)
    library(faoswsUtil)
    library(faoswsProduction)
    library(faoswsProcessing)
    library(faoswsEnsure)
    library(magrittr)
    library(dplyr)
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

message("Setting up configurations and parameters")
sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)

## Build processing parameters
processingParameters =
    productionProcessingParameters(datasetConfig = datasetConfig)

completeImputationKey = getCompleteImputationKey()

getAnimalMeatMapping = function(onlyMeatChildren = FALSE,
                                meatPattern = "^211(1|2|7).*"){
    mapping = fread("~/Downloads/animal_parent_child_mapping.csv",
                    colClasses = "character")
    if (onlyMeatChildren)
        mapping = mapping[grepl(meatPattern, measuredItemChildCPC), ]
    mapping
}


animalMeatMappingTable =
    getAnimalMeatMapping(onlyMeatChildren = TRUE) %>%
    select(measuredItemParentCPC, measuredElementParent,
           measuredItemChildCPC, measuredElementChild)


## Read the data.  The years and countries provided in the session are
## used, and the commodities in the session are somewhat
## considered. For example, if 02111 (Cattle) is in the session, then
## the session will be expanded to also include 21111.01 (meat of
## cattle, fresh or chilled), 21151 (edible offal of cattle, fresh,
## chilled or frozen), 21512 (cattle fat, unrendered), and 02951.01
## (raw hides and skins of cattle).  The measured element dimension of
## the session is simply ignored.

selectMeatCodes = function(itemCodes, meatPattern = "^211(1|2|7).*"){
    itemCodes[grepl(meatPattern, itemCodes)]
}

selectedMeatCode =
    sessionKey %>%
    expandMeatSessionSelection(oldKey = .,
                               selectedMeatTable = animalMeatMappingTable) %>%
    slot(object = ., "dimensions") %>%
    .$measuredItemCPC %>%
    slot(object = ., name = "keys") %>%
    selectMeatCodes(itemCodes = .)


for(iter in seq(selectedMeatCode)){

    currentMeatItem = selectedMeatCode[iter]
    currentMappingTable =
        animalMeatMappingTable[measuredItemChildCPC == currentMeatItem, ]
    currentAnimalItem = currentMappingTable[, measuredItemParentCPC]


########################################################################
    message("Extracting production triplet for ", currentMeatItem, " (Meat)")
    ## Get the meat formula
    meatFormulaTable =
        getYieldFormula(itemCode = currentMeatItem) %>%
        removeIndigenousBiologicalMeat(formula = .)

    if(nrow(meatFormulaTable) > 1)
        stop("Imputation should only use one formula")

    meatFormulaParameters =
        with(meatFormulaTable,
             productionFormulaParameters(datasetConfig = datasetConfig,
                                         productionCode = output,
                                         areaHarvestedCode = input,
                                         yieldCode = productivity,
                                         unitConversion = unitConversion)
             )

    ## Get the meat key
    meatKey = completeImputationKey
    meatKey@dimensions$measuredItemCPC@keys = currentMeatItem
    meatKey@dimensions$measuredElement@keys =
        with(meatFormulaParameters,
             c(productionCode, areaHarvestedCode, yieldCode,
               currentMappingTable$measuredElementChild))

    ## Get the meat data
    meatData =
        meatKey %>%
        GetData(key = .) %>%
        fillRecord(data = .) %>%
        preProcessing(data = .) %>%
        ensureProductionInputs(data = .,
                               processingParameters = processingParameters,
                               formulaParameters = meatFormulaParameters)



########################################################################
    message("Extracting production triplet for ", currentAnimalItem, " (Animal)")
    ## Get the animal formula
    animalFormulaTable =
        getYieldFormula(itemCode = currentAnimalItem) %>%
        removeIndigenousBiologicalMeat(formula = .)

    if(nrow(animalFormulaTable) > 1)
        stop("Imputation should only use one formula")

    animalFormulaParameters =
        with(animalFormulaTable,
             productionFormulaParameters(datasetConfig = datasetConfig,
                                         productionCode = output,
                                         areaHarvestedCode = input,
                                         yieldCode = productivity,
                                         unitConversion = unitConversion)
             )

    ## Get the animal key
    animalKey = completeImputationKey
    animalKey@dimensions$measuredItemCPC@keys = currentAnimalItem
    animalKey@dimensions$measuredElement@keys =
        with(animalFormulaParameters,
             c(productionCode, areaHarvestedCode, yieldCode,
               currentMappingTable$measuredElementParent))

    ## Get the animal data
    animalData =
        animalKey %>%
        GetData(key = .) %>%
        fillRecord(data = .) %>%
        preProcessing(data = .) %>%
        ensureProductionInputs(data = .,
                               processingParameters = processingParameters,
                               formulaParameters = animalFormulaParameters)



########################################################################
    message("Pulling the commodity trees ...\n")

    newCommodityTree =
        animalData %$%
        getCommodityTree(geographicAreaM49 =
                             as.character(unique(.$geographicAreaM49)),
                         timePointYears =
                             as.character(unique(.$timePointYears))) %>%
        subset(., share > 0) %>%
        setnames(.,
                 old = c("measuredItemParentCPC", "timePointYearsSP"),
                 new = c("measuredItemCPC", "timePointYears"))

########################################################################
    message("Transferring animal slaughtered from animal to all child commodity")

    ## NOTE (Michael): The only difference between the synchronisation in the
    ##                 module and the imputation module is that the mapping
    ##                 table is different and also a share applies when transfer
    ##                 parent to child.
    ##
    ## TODO (Michael): Need to check whether meat is included in this transfer!

    transferedData =
        newCommodityTree %>%
        mutate(timePointYears = as.numeric(timePointYears)) %>%
        transferParentToChild(commodityTree = .,
                              parentData = animalData,
                              selectedMeatTable = animalMeatMappingTable) %>%
        postProcessing(data = .)


########################################################################
    message("Saving the transferred data back")

    transferedData %>%
        ## TODO (Michael): Need to check whether the parent/child are
        ##                 synchronised.
        ensureProductionOutputs(data = .,
                                processingParameters = processingParameters,
                                formulaParameters = meatFormulaParameters) %>%
        postProcessing %>%
        SaveData(domain = sessionKey@domain,
                 dataset = sessionKey@dataset,
                 data = .)
}

message("Synchronise Module Executed Successfully!")
