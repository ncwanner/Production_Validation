## ###########################################################################
## Title: Impute Slaughtered Module for SWS
##
## Author: Josh Browning
## Restructured : Michael C. J. Kao
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
## 1. Save the transferred data back to the database
## 2. Impute the meat data (production/animals slaughtered/carcass weight)
## following the logic from the production imputation module.
## 3. Copy the slaughtered animal numbers in meat back to the animal commodity.
## 4. Save all three variables for meat (production/animals slaughterd/carcass
## weight) and the animals slaughtered for the animal.
## #############################################################################

## Step 0. Initial set-up

cat("Beginning impute slaughtered script...\n")
suppressMessages({
    library(faosws)
    library(faoswsFlag)
    library(faoswsUtil)
    library(faoswsImputation)
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

message("Setting up configurations and parameters\n")
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
    message("Extracting production triplet for ", currentMeatItem, "(meat)")
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
    message("Extracting production triplet for ", currentAnimalItem, "(Animal)")
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
    message("Transferring animal slaughtered from animal to meat commodity")
    ## Merge and transfer the data
    ##
    ## NOTE (Michael): The transfer can over-write official and semi-official
    ##                 figures as indicated by in the synchronise slaughtered
    ##                 module.

    slaughteredTransferedToMeat =
        transferAnimalSlaughtered(meatData = meatData,
                                  animalData = animalData,
                                  parentToChild = TRUE)
    ## Save the transfered data back
    ## NOTE (Michael): Save the data back but no need to check for protected data

    message("Saving transferred meat data back")
    slaughteredTransferedToMeat %>%
        ensureProductionOutputs(data = .,
                                processingParameters = processingParameters,
                                formulaParameters = meatFormulaParameters) %>%
        postProcessing %>%
        SaveData(domain = "agriculture",
                 dataset = "aproduction",
                 data = .)


########################################################################
    message("Start imputing meat commodity")
    ## Start the imputation
    ## Build imputation parameter
    imputationParameters =
        with(meatFormulaParameters,
             getImputationParameters(productionCode = productionCode,
                                     areaHarvestedCode = areaHarvestedCode,
                                     yieldCode = yieldCode)
             )


    ## Load the updated meat data
    updatedMeatData =
        meatKey %>%
        GetData(key = .) %>%
        fillRecord(data = .) %>%
        preProcessing(data = .) %>%
        ensureProductionInputs(data = .,
                               processingParameters = processingParameters,
                               formulaParameters = meatFormulaParameters)


    ## Process the meat data
    processedMeatData =
        updatedMeatData %>%
        denormalise(normalisedData = ., denormaliseKey = "measuredElement") %>%
        processProductionDomain(data = .,
                                processingParameters = processingParameters,
                                formulaParameters = meatFormulaParameters)

    ## Perform imputation
    meatImputed =
        imputeWithAndWithoutEstimates(
            data = processedMeatData,
            processingParameters = processingParameters,
            imputationParameters = imputationParameters,
            formulaParameters = meatFormulaParameters,
            minObsForEst = 5)


    ## Save the imputed meat back
    ##
    ## NOTE (Michael): The transfer can over-write official and semi-official
    ##                 figures as indicated by in the synchronise slaughtered
    ##                 module.

    meatImputed %>%
        normalise(.) %>%
        ## NOTE (Michael): This test currently fails occasionally
        ##                 because it can not overwrite the ('I', '-')
        ##                 flag for imputed value from the old system.
        ##                 This is an error in the system as the flag
        ##                 ('I', '-') should be replaced with ('E', 'e')
        ##                 which can be over-written.
        ##
        ## NOTE (Michael): Need to apply the formula for from the meat.
        ensureProductionOutput(data = .,
                               processingParameters = processingParameters,
                               formulaParameters = formulaParameters) %>%
        filter(flagMethod %in% c("i", "t", "e", "n", "u")) %>%
        postProcessing(data = .) %>%
        SaveData(domain = subKey@domain,
                 dataset = subKey@dataset,
                 data = .)

########################################################################
    message("Transfer animal slaughtered back from meat to animal commodity")
    ## Transfer the animal slaughtered from meat back to animal
    ##
    ## NOTE (Michael): The transfer can over-write official and semi-official
    ##                 figures as indicated by in the synchronise slaughtered
    ##                 module.

    slaughteredTransferedBackToAnimal =
        meatImputed %>%
        normalise(denormalisedData = .) %>%
        transferAnimalSlaughtered(meatData = .,
                                  animalData = animalData,
                                  parentToChild = FALSE)


########################################################################
    message("Saving transferred animal data back")
    slaughteredTransferedBackToAnimal %>%
        ensureProductionOutputs(data = .,
                                processingParameters = processingParameters,
                                formulaParameters = meatFormulaParameters) %>%
        postProcessing(data = .) %>%
        SaveData(domain = "agriculture",
                 dataset = "aproduction",
                 data = .)


}
message("Imputation Module Executed Successfully!")



