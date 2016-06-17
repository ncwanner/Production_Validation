##' # Imputation and Synchronisation of Livestock Commodities
##'
##' **Author: Josh Browning, Michael C. J. Kao**
##'
##' **Description:**
##'
##' The animals slaughtered for production of meat, offals, fats and hides must
##' be available before running the production imputation code. These numbers,
##' however, are not guaranteed to be available, and in the case of missing
##' data, an imputation method must be applied.
##'
##' The decision was to use the production figures of meat, if available, to
##' compute the missing animals slaughtered. If these figures are also missing,
##' they should be imputed using the production imputation methodology. Of
##' course, in the case of currently available data in the animal element, that
##' data should be transferred to the quantity of animals slaughtered for meat
##' and then the imputation ran. We also decided to save the imputations for
##' meat so as to retain consistency with the animal figures.
##'
##' Although the procedure is called transfer, however, the value is actually
##' calculated. To transfer value from animal (parent) to meat (child), we copy
##' the value, then multiplied by a `share`. The meaning of the variable is the
##' share of the slaughtered animal that is used as input for the children. In
##' most cases they are 100%, however, take cattle in India for example, they
##' can be less then 100 as not all cattle slaughtered are used to produce meat
##' due to the holy nature of the animal.
##'
##' **Inputs:**
##'
##' * Production domain
##' * Livestock Element Mapping Table
##' * Identity Formula table
##' * Share table
##'
##' **Steps:**
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
##'
##' **Flag Changes:**
##'
##' | Procedure | Observation Status Flag | Method Flag|
##' | --- | --- | --- |
##' | Tranasfer between animal and meat commodity | `<Same as origin>` | c |
##' | Balance by Production Identity | I | i |
##' | Imputation | I | e |
##'
##' **NOTE (Michael): Currently the transfer has flag 'c' indicating it is
##' copied, however, they should be replaced with a new flag as it is calculated
##' by not by identity.**
##'
##' ---

##' ## Initialisation
##'

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
if(!imputationSelection %in% c("session", "all"))
    stop("Incorrect imputation selection specified")


##' Build processing parameters
processingParameters =
    productionProcessingParameters(datasetConfig = datasetConfig)

##' Obtain the complete imputation key
completeImputationKey = getCompleteImputationKey("production")

##' Extract the animal parent to child commodity mapping table
##'
##' This table contains the parent item/element code which maps to the child
##' item/element code. For example, the slaughtered animal element for cattle is
##' 5315, while the slaughtered animal for cattle meat is 5320.
##'

## NOTE (Michael): Ideally, the two elements should be merged and have a single
##                 code in the classification. This will eliminate the change of
##                 code in the transfer procedure.
animalMeatMappingTable =
    getAnimalMeatMapping(R_SWS_SHARE_PATH = R_SWS_SHARE_PATH,
                         onlyMeatChildren = FALSE) %>%
    select(measuredItemParentCPC, measuredElementParent,
           measuredItemChildCPC, measuredElementChild)


##' Here we expand the session to include all the parent and child items.
##'
##' For example, if 02111 (Cattle) is in the session, then the session will be
##' expanded to also include 21111.01 (meat of cattle, freshor chilled), 21151
##' (edible offal of cattle, fresh, chilled or frozen), 21512 (cattle fat,
##' unrendered), and 02951.01 (raw hides and skins of cattle).
##'
##' The elements are also expanded to the required triplet.

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
           all = livestockImputationItems)


##' ---
##' ## Perform Synchronisation and Imputation

##' Here we iterate through the the meat item to perform the steps described in
##' the description. Essentially, we are looping over different livestock trees.


for(iter in seq(selectedMeatCode)){
    message("Processing livestock tree (", iter, " out of ",
            length(selectedMeatCode), ")")
    message("Step 1: Extract Transfer Animal Slaughtered from animal",
            " commodity to Meat")
    currentMeatItem = selectedMeatCode[iter]
    currentMappingTable =
        animalMeatMappingTable[measuredItemChildCPC == currentMeatItem, ]
    currentAnimalItem = currentMappingTable[, measuredItemParentCPC]
    currentAllDerivedProduct =
        animalMeatMappingTable[measuredItemParentCPC == currentAnimalItem,
                               measuredItemChildCPC]
    currentNonMeatItem =
        currentAllDerivedProduct[currentAllDerivedProduct != currentMeatItem]


    message("\tExtracting the shares tree")
    shareData =
        getShareData(geographicAreaM49 =
                         getQueryKey("geographicAreaM49", completeImputationKey),
                     measuredItemChildCPC = currentAllDerivedProduct,
                     measuredItemParentCPC = currentAnimalItem,
                     timePointYearsSP =
                         getQueryKey("timePointYears", completeImputationKey)) %>%
        setnames(x = .,
                 old = c("Value", "timePointYearsSP"),
                 new = c("share", "timePointYears")) %>%
        mutate(timePointYears = as.numeric(timePointYears))

    ## ---------------------------------------------------------------------
    message("\tExtracting production triplet for item ", currentMeatItem,
            " (Meat)")
    ## Get the meat formula
    meatFormulaTable =
        getProductionFormula(itemCode = currentMeatItem) %>%
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
        preProcessing(data = .) %>%
        denormalise(normalisedData = .,
                    denormaliseKey = "measuredElement") %>%
        createTriplet(data = .,
                      formula = meatFormulaTable) %>%
        processProductionDomain(data = .,
                                processingParameters = processingParameters,
                                formulaParameters = meatFormulaParameters) %>%
        ## NOTE (Michael): The function name should be generalised, here we are
        ##                 actually removing animal slaughtered values that were
        ##                 previously copied from the animal (parent).
        removeCalculated(data = .,
                         valueVar =
                             meatFormulaParameters$areaHarvestedValue,
                         observationFlagVar =
                             meatFormulaParameters$areaHarvestedObservationFlag,
                         methodFlagVar =
                             meatFormulaParameters$areaHarvestedMethodFlag,
                         calculatedMethodFlag = "c") %>%
        ## NOTE (Michael): Here we are removing manual estimates of animal
        ##                 slaughtered of the meat. If there is to be any manual
        ##                 estimates (which there shouldn't), it should be
        ##                 inputted in the animal slaughtered element of the
        ##                 animal (parent) commodity.
        removeImputationEstimation(data = .,
                                   valueVar =
                                       meatFormulaParameters$areaHarvestedValue,
                                   observationFlagVar =
                                       meatFormulaParameters$areaHarvestedObservationFlag,
                                   methodFlagVar =
                                       meatFormulaParameters$areaHarvestedMethodFlag,
                                   imputationEstimationObservationFlag = "E",
                                   imputationEstimationMethodFlag = "f") %>%
        ## NOTE (Michael): The function name should be generalised, here we are
        ##                 actually removing values imported from the old
        ##                 system. It was indicated that the values are old and
        ##                 can be over-written.
        removeCalculated(data = .,
                         valueVar =
                             meatFormulaParameters$areaHarvestedValue,
                         observationFlagVar =
                             meatFormulaParameters$areaHarvestedObservationFlag,
                         methodFlagVar =
                             meatFormulaParameters$areaHarvestedMethodFlag,
                         calculatedMethodFlag = "-") %>%
        ensureProductionInputs(data = .,
                               processingParameters = processingParameters,
                               formulaParameters = meatFormulaParameters,
                               normalised = FALSE) %>%
        normalise(denormalisedData = .,
                  removeNonExistingRecords = FALSE)

    ## ---------------------------------------------------------------------
    message("\tExtracting production triplet for item ", currentAnimalItem,
            " (Animal)")
    ## Get the animal formula
    animalFormulaTable =
        getProductionFormula(itemCode = currentAnimalItem) %>%
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
        preProcessing(data = .) %>%
        denormalise(normalisedData = .,
                    denormaliseKey = "measuredElement") %>%
        createTriplet(data = .,
                      formula = animalFormulaTable) %>%
        processProductionDomain(data = .,
                                processingParameters = processingParameters,
                                formulaParameters = animalFormulaParameters) %>%
        ensureProductionInputs(data = .,
                               processingParameters = processingParameters,
                               formulaParameters = animalFormulaParameters,
                               normalised = FALSE) %>%
        normalise(denormalisedData = .,
                  removeNonExistingRecords = FALSE)

    if(length(currentNonMeatItem) > 0){

        ## NOTE (Michael): We need to test whether the commodity has non-meat
        ##                 item. For example, the commodity "Other Rodent"
        ##                 (02192.01) does not have non-meat derived products
        ##                 and thus we do not need to perform the action.

        message("\tExtracting production triplet for item ",
                paste0(currentNonMeatItem, collapse = ", "),
                " (Non-meat Child)")
        ## Get the non Meat formula
        nonMeatFormulaTable =
            getProductionFormula(itemCode = currentNonMeatItem) %>%
            removeIndigenousBiologicalMeat(formula = .)

        ## Build the non meat key
        nonMeatKey = completeImputationKey
        nonMeatKey@dimensions$measuredItemCPC@keys = currentNonMeatItem
        nonMeatKey@dimensions$measuredElement@keys =
            with(nonMeatFormulaTable,
                 unique(c(input, output, productivity,
                          currentMappingTable$measuredElementChild)))

        ## Get the non meat data
        ##
        ## HACK (Michael): Current we don't test the input of non-meat item.
        ##                 This is because the processProductionDomain and
        ##                 ensureProductionInputs only work for triplets of a
        ##                 single item. However, in the non-meat data, there are
        ##                 more than one item and thus we are unable to process
        ##                 and test them.
        nonMeatData =
            nonMeatKey %>%
            GetData(key = .) %>%
            preProcessing(data = .) %>%
            denormalise(normalisedData = .,
                        denormaliseKey = "measuredElement") %>%
            createTriplet(data = .,
                          formula = nonMeatFormulaTable) %>%
            normalise(denormalisedData = .,
                      removeNonExistingRecords = FALSE)

    }

    ## ---------------------------------------------------------------------
    message("\tTransferring animal slaughtered from animal to meat commodity")
    animalMeatMappingShare =
        merge(currentMappingTable, shareData, all.x = TRUE,
              by = c("measuredItemParentCPC", "measuredItemChildCPC"))

    ## Transfer the animal slaughtered number from animal to the meat.
    slaughteredTransferedToMeatData =
        transferParentToChild(parentData = animalData,
                              childData = meatData,
                              mappingTable = animalMeatMappingShare,
                              parentToChild = TRUE)

    ## ---------------------------------------------------------------------
    message("Step 2: Perform Imputation on the Meat Triplet")

    ## Start the imputation
    ## Build imputation parameter
    imputationParameters =
        with(meatFormulaParameters,
             getImputationParameters(productionCode = productionCode,
                                     areaHarvestedCode = areaHarvestedCode,
                                     yieldCode = yieldCode)
             )

    ## Perform imputation using the standard imputation function
    ##
    message("\tPerforming Imputation")

    meatImputed =
        slaughteredTransferedToMeatData %>%
        expandYear(data = .,
                   areaVar = processingParameters$areaVar,
                   elementVar = processingParameters$elementVar,
                   itemVar = processingParameters$itemVar,
                   valueVar = processingParameters$valueVar) %>%
        denormalise(normalisedData = .,
                    denormaliseKey = "measuredElement",
                    fillEmptyRecord = TRUE) %>%
        processProductionDomain(data = .,
                                processingParameters = processingParameters,
                                formulaParameters = meatFormulaParameters) %>%
        imputeProductionTriplet(data = .,
                                processingParameters = processingParameters,
                                imputationParameters = imputationParameters,
                                formulaParameters = meatFormulaParameters) %>%
        ensureProductionOutputs(data = .,
                                processingParameters = processingParameters,
                                formulaParameters = meatFormulaParameters,
                                normalised = FALSE) %>%
        normalise



    ## ---------------------------------------------------------------------
    message("Step 3: Transfer animal slaughtered back from meat to animal commodity")

    ## Transfer the animal slaughtered from meat back to animal, this can be
    ## done by specifying parentToChild equal to FALSE.
    ##
    ## NOTE (Michael): We only subset the new calculated or imputed values to be
    ##                 transfer back to the animal (parent) commodity. See issue
    ##                 #180.
    ##
    ## NOTE (Michael): Since the animal element is not imputed nor balanced , we
    ##                 will not test whether it is imputed or the identity
    ##                 calculated.
    slaughteredTransferedBackToAnimalData =
        meatImputed %>%
        filter(., flagMethod == "i" |
                  (flagObservationStatus == "I" &
                   flagMethod == "e")) %>%
        transferParentToChild(parentData = animalData,
                              childData = .,
                              mappingTable = animalMeatMappingShare,
                              parentToChild = FALSE) %>%
        ensureProductionOutputs(data = .,
                                processingParameters = processingParameters,
                                formulaParameters = animalFormulaParameters,
                                testImputed = FALSE,
                                testCalculated = FALSE)

    ## ---------------------------------------------------------------------

    if(length(currentNonMeatItem) > 0){

        ## NOTE (Michael): We need to test whether the commodity has non-meat
        ##                 item. For example, the commodity "Other Rodent"
        ##                 (02192.01) does not have non-meat derived products
        ##                 and thus we do not need to perform the action.
    message("Step 4: Transfer Animal Slaughtered to All Child Commodities")

    nonMeatMappingTable =
        animalMeatMappingTable[measuredItemChildCPC %in% currentNonMeatItem, ]

    animalNonMeatMappingShare =
        merge(nonMeatMappingTable, shareData, all.x = TRUE,
              by = c("measuredItemParentCPC", "measuredItemChildCPC"))

    slaughteredTransferToNonMeatChildData =
        transferParentToChild(parentData = slaughteredTransferedBackToAnimalData,
                              childData = nonMeatData,
                              mappingTable = animalNonMeatMappingShare,
                              parentToChild = TRUE)
    }

    ## ---------------------------------------------------------------------
    message("\tTesting transfers are applied correctly")
    ## WARNING (Michael): We currently only check the synchronisation between
    ##                    animal and the meat as this processed is applied in
    ##                    the module. The animal slaughtered si transferred from
    ##                    animal to non-meat items, but not the reverse so we
    ##                    can not expect them to be synchronised. However, we
    ##                    need to also ensure the synchronisation happen between
    ##                    other the animal and non-meat child. How to do this
    ##                    specifically, I have no immediate idea. This is
    ##                    related to issue 178.
    ##
    ensureCorrectTransfer(parentData = slaughteredTransferedBackToAnimalData,
                          childData = meatImputed,
                          mappingTable = animalMeatMappingShare,
                          returnData = FALSE)


    message("\tSaving the synchronised and imputed data back")
    if(length(currentNonMeatItem) > 0){
        syncedData = rbind(meatImputed,
                           slaughteredTransferedBackToAnimalData,
                           slaughteredTransferToNonMeatChildData)
    } else {
        syncedData = rbind(meatImputed,
                           slaughteredTransferedBackToAnimalData)
    }

    syncedData %>%
        ## NOTE (Michael): The transfer can over-write official and
        ##                 semi-official figures in the processed commodities as
        ##                 indicated by in the previous synchronise slaughtered
        ##                 module.
        ##
        removeInvalidDates(data = ., context = sessionKey) %>%
        postProcessing %>%
        SaveData(domain = sessionKey@domain,
                 dataset = sessionKey@dataset,
                 data = .)

    message("\nSynchronisation and Imputation Completed for\n",
            "Animal Parent: ", currentAnimalItem, "\n",
            "Meat Child: ", currentMeatItem, "\n",
            "Non-meat Child: ", paste0(currentNonMeatItem, collapse = ", "), "\n",
            rep("-", 80), "\n")

}

