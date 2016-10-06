
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


##' Load and check the computation parameters
imputationSelection = swsContext.computationParams$imputation_selection
if(!imputationSelection %in% c("session", "all"))
    stop("Incorrect imputation selection specified")


##' Get data configuration and session
sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)

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


##' Here we expand the session to include all the parent and child items. That
##' is, we expand to the particular livestock tree.
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





for(iter in seq(selectedMeatCode)){
    message("Processing livestock tree (", iter, " out of ",
            length(selectedMeatCode), ")")
    message("Step 1: Extract Transfer Animal Slaughtered from animal",
            " commodity to Meat")
    
    set.seed(070416)
    
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
    message("\tExtracting production triplet "," for item ", currentMeatItem, " (Meat)")
    ## Get the meat formula
    meatFormulaTable =
        getProductionFormula(itemCode = currentMeatItem) %>%
        removeIndigenousBiologicalMeat(formula = .)
    
    ## NOTE (Michael): Imputation should be performed on only 1 formula, if
    ##                 there are multiple formulas, they should be calculated
    ##                 based on the values imputed. For example, if one of the
    ##                 formula has production in tonnes while the other has
    ##                 production in kilo-gram, then we should impute the
    ##                 production in tonnes, then calculate the production in
    ##                 kilo-gram.
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
        GetData(key = .)
    
    
    imputationParameters =
        with(meatFormulaParameters,
             getImputationParameters(productionCode = productionCode,
                                     areaHarvestedCode = areaHarvestedCode,
                                     yieldCode = yieldCode)
        )
    
    
    
    
    
    meatData=denormalise(meatData,denormaliseKey = "measuredElement")
    
    
    invalidData=ensureProductionBalanced(data = meatData,
                             areaVar = imputationParameters$areaHarvestedParams$imputationValueColumn,
                             yieldVar = imputationParameters$yieldParams$imputationValueColumn,
                             prodVar = imputationParameters$productionParams$imputationValueColumn,
                             conversion = meatFormulaParameters$unitConversion,
                             returnData = FALSE,
                             getInvalidData = TRUE,
                             normalised = FALSE)
    
    
    
    invalidData[,":="(c(imputationParameters$yieldParams$imputationValueColumn),
                   c((get(imputationParameters$productionParams$imputationValueColumn)*1000)/get(imputationParameters$areaHarvestedParams$imputationValueColumn))
    )]
    
    invalidData[,":="(c(imputationParameters$yieldParams$imputationFlagColumn),
                      aggregateObservationFlag(get(imputationParameters$areaHarvestedParams$imputationFlagColumn),
                                               get(imputationParameters$productionParams$imputationFlagColumn)))]
    
    
    invalidData[,":="(c(imputationParameters$yieldParams$imputationMethodColumn),
                      c("i")
    )]
    
    
        invalidData %>%
        normalise(.) %>%
        SaveData(domain = sessionKey@domain,
                 dataset = sessionKey@dataset,
                 data = .)
    

    
}


for(iter in seq(selectedMeatCode)){
    message("Processing livestock tree (", iter, " out of ",
            length(selectedMeatCode), ")")
    message("Step 1: Extract Transfer Animal Slaughtered from animal",
            " commodity to Meat")
    
    set.seed(070416)
    
    currentMeatItem = selectedMeatCode[2]
    
    
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
    message("\tExtracting production triplet "," for item ", currentMeatItem, " (Meat)")
    ## Get the meat formula
    meatFormulaTable =
        getProductionFormula(itemCode = currentMeatItem) %>%
        removeIndigenousBiologicalMeat(formula = .)
    
    ## NOTE (Michael): Imputation should be performed on only 1 formula, if
    ##                 there are multiple formulas, they should be calculated
    ##                 based on the values imputed. For example, if one of the
    ##                 formula has production in tonnes while the other has
    ##                 production in kilo-gram, then we should impute the
    ##                 production in tonnes, then calculate the production in
    ##                 kilo-gram.
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
        GetData(key = .)
    
    
    imputationParameters =
        with(meatFormulaParameters,
             getImputationParameters(productionCode = productionCode,
                                     areaHarvestedCode = areaHarvestedCode,
                                     yieldCode = yieldCode)
        )
    
   currentRange=rangeCarcassWeight[meatItemCPC==currentMeatItem,] 
    
   meatData=denormalise(meatData, denormaliseKey = "measuredElement")
   
 ##  meatData[get(imputationParameters$yieldParams$imputationValueColumn)==0,
 ##  imputationParameters$yieldParams$imputationValueColumn:=NA_real_]
  
   
 ## median byKey (geo+item)
 ##  
 ## meatData[, ":="("medianByKey", median(.SD[[imputationParameters$yieldParams$imputationValueColumn]],na.rm=TRUE)),
 ##          by=c(imputationParameters$yieldParams$byKey)]
 ## 
   
   
 ## meatData[, ":="("medianYield", median(get(imputationParameters$yieldParams$imputationValueColumn),na.rm=TRUE))]
   
 ## outOfRange[, medianYield:=NULL]   
  
   
   
    outOfRange=meatData[get(imputationParameters$yieldParams$imputationValueColumn) >  currentRange[,carcassWeightMax] |
                       get(imputationParameters$yieldParams$imputationValueColumn) <  currentRange[,carcassWeightMin],]
 

 
 
 ## Replace the values of carcass weight outside from the range with the overall median by commodity
 ##outOfRange[,":="(c(imputationParameters$yieldParams$imputationValueColumn), c(medianYield))]
 
 
 ## Replace the values of carcass weight outside from the range with the extremes of the range
    

    
 ## Impose the outOfRange values below the minimum equal to the
 ## lower extreme of the range and  the outOfRange Values up the max equal to upper extreme of the range
    
 outOfRange[get(imputationParameters$yieldParams$imputationValueColumn) >  currentRange[,carcassWeightMax]
            & get(imputationParameters$yieldParams$imputationFlagColumn) !="M"
            & get(imputationParameters$yieldParams$imputationMethodColumn) !="u",
            ":="(c(imputationParameters$yieldParams$imputationValueColumn), 
                 c(currentRange[,carcassWeightMax]))]
 
 outOfRange[get(imputationParameters$yieldParams$imputationValueColumn) >  currentRange[,carcassWeightMin]
            & get(imputationParameters$yieldParams$imputationFlagColumn) !="M"
            & get(imputationParameters$yieldParams$imputationMethodColumn) !="u",
            ":="(c(imputationParameters$yieldParams$imputationValueColumn), 
                 currentRange[,carcassWeightMin])]
 
 ##Adjust the Flags for the new carcass weights
 
 outOfRange[,":="(c(imputationParameters$yieldParams$imputationFlagColumn), c("I"))]
 outOfRange[,":="(c(imputationParameters$yieldParams$imputationMethodColumn), c("e"))]
 
 

 ## We should free the number of anumal slaughtered and recalculate this variable as identity
 
 outOfRange[get(imputationParameters$yieldParams$imputationFlagColumn) !="M"
            & get(imputationParameters$yieldParams$imputationMethodColumn) !="u",
            ":="(c(imputationParameters$areaHarvestedParams$imputationValueColumn),
                  c((get(imputationParameters$productionParams$imputationValueColumn)
                     *1000)/get(imputationParameters$yieldParams$imputationValueColumn)
 ))]
 
 
 ## Adjust the ObservationFlag for animal slaughterd
 
 outOfRange[get(imputationParameters$yieldParams$imputationFlagColumn) !="M"
            & get(imputationParameters$yieldParams$imputationMethodColumn) !="u",
            ":="(c(imputationParameters$areaHarvestedParams$imputationFlagColumn),
                  aggregateObservationFlag(get(imputationParameters$yieldParams$imputationFlagColumn),
                  get(imputationParameters$productionParams$imputationFlagColumn)) )]
 
 ## Adjust the methodFlag for animal slaughterd
 
 outOfRange[get(imputationParameters$yieldParams$imputationFlagColumn) !="M"
            & get(imputationParameters$yieldParams$imputationMethodColumn) !="u",
            ":="(c(imputationParameters$areaHarvestedParams$imputationMethodColumn), c("i") )]
 
 

 
 outOfRange %>%
     normalise(.) %>%
     SaveData(domain = sessionKey@domain,
              dataset = sessionKey@dataset,
              data = .)
 
 
 
     
    }