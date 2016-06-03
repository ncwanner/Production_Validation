##' # Synchronise and Impute Slaughtered
##'
##' ## Author: Josh Browning, Michael C. J. Kao
##'
##' ## Description:
##'
##' The animals slaughtered for production of meat, offals, fats and hides must
##' be available before running the production imputation code. These numbers,
##' however, are not guaranteed to be available, and in the case of missing
##' data, an imputation method must be applied. The decision was to use the
##' production figures of meat, if available, to compute the missing animals
##' slaughtered. If these figures are also missing, they should be imputed using
##' the production imputation methodology. Of course, in the case of currently
##' available data in the animal element, that data should be transferred to the
##' quantity of animals slaughtered for meat and then the imputation ran. We
##' also decided to save the imputations for meat so as to retain consistency
##' with the animal figures.
##'
##' ## Steps:
##'
##' 1. Transfer the animal slaughtered from animal commodity (parent) to the
##'    meat commodity (child)
##'
##' 2. Impute the meat triplet (production/animal slaughtered/carcass weight)
##'    based on the same logic as all other production imputation procedure.
##'
##' 3. Transfer the slaughtered animal from the meat back to the animal, as now
##'    certain slaughtered animal is imputed in step 2.
##'
##' 4. Transfer the slaughtered animal from the animal to all other child
##'    commodities. This includes items such as offals, fats and hides.



message("Step 0: Setup")

##' Load the libraries
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

##' Get the shared path
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

##' Load debugging setting if in debug mode.
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


##' Load the computation parameters
imputationSelection = swsContext.computationParams$imputation_selection
sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)

## Select the item list based on user input parameter
if(!imputationSelection %in% c("session", "all", "missing_items"))
    stop("Incorrect imputation selection specified")


##' Build processing parameters
processingParameters =
    productionProcessingParameters(datasetConfig = datasetConfig)

##' Obtain the complete imputation key
completeImputationKey = getCompleteImputationKey()

## TODO (Michael): This is to be removed.
## getAnimalMeatMapping = function(onlyMeatChildren = FALSE,
##                                 meatPattern = "^211(1|2|7).*"){
##     mapping = fread("~/Downloads/animal_parent_child_mapping.csv",
##                     colClasses = "character")
##     if(onlyMeatChildren)
##         mapping =
##             subset(mapping,
##                    measuredItemChildCPC %in%
##                    selectMeatCodes(measuredItemChildCPC,
##                                     meatPattern = meatPattern))
##     mapping
## }

##' Extract the animal parent to child commodity mapping table
##'
##' This table contains the parent item/element code which maps to the child
##' item/element code. For example, the slaughtered animal element for cattle is
##' 5315, while the slaughtered animal for cattle meat is 5320.
##'

## NOTE (Michael): The two elements should be merged and have a single code.
##                 This will eliminate the change of code in the transfer
##                 procedure.
animalMeatMappingTable =
    getAnimalMeatMapping(onlyMeatChildren = FALSE) %>%
    select(measuredItemParentCPC, measuredElementParent,
           measuredItemChildCPC, measuredElementChild)


##' Here we expand the session to include all the parent and child items.
##'
##' For example, if 02111 (Cattle) is in the session, then the session will be
##' expanded to also include 21111.01 (meat of cattle, freshor chilled), 21151
##' (edible offal of cattle, fresh, chilled or frozen), 21512 (cattle fat,
##' unrendered), and 02951.01 (raw hides and skins of cattle).
##'
##' The elements are also expanded

livestockImputationItems =
    completeImputationKey %>%
    expandMeatSessionSelection(oldKey = .,
                               selectedMeatTable = animalMeatMappingTable) %>%
    getQueryKey("measuredItemCPC", datasetkey = .) %>%
    selectMeatCodes(itemCodes = .)

sessionItems =
    sessionKey %>%
    expandMeatSessionSelection(oldKey = .,
                               selectedMeatTable = animalMeatMappingTable) %>%
    getQueryKey("measuredItemCPC", datasetkey = .) %>%
    selectMeatCodes(itemCodes = .)

##' Select the range of items based on the computational parameter.
selectedMeatCode =
    switch(imputationSelection,
           session = sessionItems,
           all = nonLivestockImputationItems)


##' Iterate through the selected meat items and transfer the the animal
##' slaughtered from the animal parent to the child commodity such as offals and
##' skins.

for(iter in seq(selectedMeatCode)){

    message("Step 1: Extract Transfer Animal Slaughtered from animal commodity to Meat")
    currentMeatItem = selectedMeatCode[iter]
    currentMappingTable =
        animalMeatMappingTable[measuredItemChildCPC == currentMeatItem, ]
    currentAnimalItem = currentMappingTable[, measuredItemParentCPC]


    ## ---------------------------------------------------------------------
    message("\tExtracting production triplet for item ", currentMeatItem,
            " (Meat)")
    ## Get the meat formula
    meatFormulaTable =
        getYieldFormula(itemCode = currentMeatItem) %>%
        removeIndigenousBiologicalMeat(formula = .)

    if(nrow(meatFormulaTable) > 1)
        stop("Imputation should only use one formula")

    ## Create the formula parameter list
    meatFormulaParameters =
        with(meatFormulaTable,
             productionFormulaParameters(datasetConfig = datasetConfig,
                                         productionCode = output,
                                         areaHarvestedCode = input,
                                         yieldCode = productivity,
                                         unitConversion = unitConversion)
             )

    ## Get the meat key, we take the complete key and then modify the element
    ## and item dimension to extract the current meat item and it's
    ## corresponding elements.
    ##
    ## NOTE (Michael): We extract the triplet so that we can perform the check
    ##                 on whether the triplet are balanced already. Eventhough
    ##                 only the animal slaughtered element is transferred.
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
        createTriplet(data = ., formula = meatFormulaTable) %>%
        ensureProductionInputs(data = .,
                               processingParameters = processingParameters,
                               formulaParameters = meatFormulaParameters)



    ## ---------------------------------------------------------------------
    message("\tExtracting production triplet for item ", currentAnimalItem,
            " (Animal)")
    ## Get the animal formula
    animalFormulaTable =
        getYieldFormula(itemCode = currentAnimalItem) %>%
        removeIndigenousBiologicalMeat(formula = .)

    if(nrow(animalFormulaTable) > 1)
        stop("Imputation should only use one formula")

    ## Create the formula parameter list
    animalFormulaParameters =
        with(animalFormulaTable,
             productionFormulaParameters(datasetConfig = datasetConfig,
                                         productionCode = output,
                                         areaHarvestedCode = input,
                                         yieldCode = productivity,
                                         unitConversion = unitConversion)
             )

    ## Get the animal key, we take the complete key and then modify the element
    ## and item dimension to extract the current meat item and it's
    ## corresponding elements.
    ##
    ## NOTE (Michael): We extract the triplet so that we can perform the check
    ##                 on whether the triplet are balanced already. Eventhough
    ##                 only the animal slaughtered element is transferred.
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
        createTriplet(data = ., formula = animalFormulaTable) %>%
        ensureProductionInputs(data = .,
                               processingParameters = processingParameters,
                               formulaParameters = animalFormulaParameters)


    ## ---------------------------------------------------------------------
    message("\tTransferring animal slaughtered from animal to meat commodity")
    ## Transfer the animal slaughtered number from animal to the meat.
    slaughteredTransferedToMeat =
        transferAnimalSlaughtered(meatData = meatData,
                                  animalData = animalData,
                                  mappingTable = currentMappingTable,
                                  parentToChild = TRUE)

    message("\tSaving transferred meat data back to the database")
    ## Save the transfered data back
    ##
    ## NOTE (Michael): The transfer can over-write official and semi-official
    ##                 figures as indicated by in the synchronise slaughtered
    ##                 module.
    slaughteredTransferedToMeat %>%
        ensureProductionOutputs(data = .,
                                processingParameters = processingParameters,
                                formulaParameters = meatFormulaParameters) %>%
        postProcessing %>%
        SaveData(domain = sessionKey@domain,
                 dataset = sessionKey@dataset,
                 data = .)


    ## ---------------------------------------------------------------------
    message("Step 2: Perform Imputation on the Meat Triplet")

    ## NOTE (Michael): To perform the imputation, we read the transferred data
    ##                 from the data base rather than use the data obtained
    ##                 above. However, maybe we can process the batch and then
    ##                 save the whold data back in one iteration to avoid
    ##                 read/write overhead.


    ## Start the imputation
    ## Build imputation parameter
    imputationParameters =
        with(meatFormulaParameters,
             getImputationParameters(productionCode = productionCode,
                                     areaHarvestedCode = areaHarvestedCode,
                                     yieldCode = yieldCode)
             )


    ## Load the updated meat data from the database
    updatedMeatData =
        meatKey %>%
        GetData(key = .) %>%
        fillRecord(data = .) %>%
        preProcessing(data = .) %>%
        ensureProductionInputs(data = .,
                               processingParameters = processingParameters,
                               formulaParameters = meatFormulaParameters)


    ## Denormalise and then process the meat data
    processedMeatData =
        updatedMeatData %>%
        denormalise(normalisedData = .,
                    denormaliseKey = "measuredElement",
                    fillEmptyRecords = TRUE) %>%
        createTriplet(data = ., formula = meatFormulaTable) %>%
        processProductionDomain(data = .,
                                processingParameters = processingParameters,
                                formulaParameters = meatFormulaParameters)

    ## Perform imputation using the standard imputation function
    message("\tPerfoming Imputation")
    meatImputed =
        imputeWithAndWithoutEstimates(
            data = processedMeatData,
            processingParameters = processingParameters,
            imputationParameters = imputationParameters,
            formulaParameters = meatFormulaParameters,
            minObsForEst = 5)


    message("\tSaving imputed meat data back")
    ## Save the imputed meat back
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
                               formulaParameters = meatFormulaParameters) %>%
        ## filter(flagMethod %in% c("i", "t", "e", "n", "u")) %>%
        postProcessing(data = .) %>%
        SaveData(domain = sessionKey@domain,
                 dataset = sessionKey@dataset,
                 data = .)

    ## ---------------------------------------------------------------------
    message("Step 3: Transfer animal slaughtered back from meat to animal commodity")


    message("\tTransferring animal slaughtered from meat to animal commodity")
    ## Transfer the animal slaughtered from meat back to animal, this can be
    ## done by specifying parentToChild equal to FALSE.
    slaughteredTransferedBackToAnimal =
        meatImputed %>%
        normalise(denormalisedData = .) %>%
        transferAnimalSlaughtered(meatData = .,
                                  animalData = animalData,
                                  mappingTable = currentMappingTable,
                                  parentToChild = FALSE)


    ## ---------------------------------------------------------------------
    message("\tSaving transferred animal data back")
    ## NOTE (Michael): The transfer can over-write official and semi-official
    ##                 figures as indicated by in the synchronise slaughtered
    ##                 module.

    slaughteredTransferedBackToAnimal %>%
        ensureProductionOutputs(data = .,
                                processingParameters = processingParameters,
                                formEulaParameters = meatFormulaParameters) %>%
        postProcessing(data = .) %>%
        SaveData(domain = sessionKey@domain,
                 dataset = sessionKey@dataset,
                 data = .)

    ## ---------------------------------------------------------------------
    message("Step 4: Transfer Animal Slaughtered to All Child Commodities")
    message("\tPulling the commodity trees")

    currentCommodityTree =
        animalData %$%
        getCommodityTree(geographicAreaM49 =
                             as.character(unique(.$geographicAreaM49)),
                         timePointYears =
                             as.character(unique(.$timePointYears))) %>%
        subset(., share > 0 &
                  measuredItemParentCPC == currentAnimalItem &
                  measuredItemChildCPC != currentMeatItem) %>%
        setnames(.,
                 old = c("measuredItemParentCPC", "timePointYearsSP"),
                 new = c("measuredItemCPC", "timePointYears")) %>%
        mutate(timePointYears = as.numeric(timePointYears))



    message("\tTransferring animal slaughtered from animal to the following\n",
            "child commodities (Meat excluded):\n\t",
            paste0(unique(currentCommodityTree$measuredItemChildCPC),
                   collapse = ", "))

    transferedData =
        currentCommodityTree %>%
        transferParentToChild(commodityTree = .,
                              parentData = animalData,
                              selectedMeatTable = animalMeatMappingTable) %>%
        postProcessing(data = .)


    ## ---------------------------------------------------------------------
    message("\tSaving the transferred data back")

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

    message("\nSynchronisation and Imputation Completed for\n",
            "Animal Parent: ", currentAnimalItem, "\n",
            "Meat Child: ", currentMeatItem, "\n",
            rep("-", 80), "\n")

}




