##' # Imputation and Synchronisation of Livestock Commodities
##'
##' **Author: Francesca Rosa**
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
##' * Complete Key Table
##' * Livestock Element Mapping Table
##' * Identity Formula table
##' * Share table
##' * Elements code table
##' * Range Carcass Weight table
##'
##' **Steps:**
##' 
##' 1. Impute Livestock Numbers
##' 
##' 2. Impute Number of Slaughtered animal (assiciated to the animal item)
##'
##' 3. Transfer the animal slaughtered from animal commodity (parent) to the
##'    meat commodity (child)
##'
##' 4. Impute the meat triplet (production/animal slaughtered/carcass weight)
##'    based on the same logic as all other production imputation procedure.
##'
##' 5. Transfer the slaughtered animal from the meat back to the animal, as now
##'    certain slaughtered animal is imputed in step 3.
##'
##' 6. Transfer the slaughtered animal from the animal to all other child
##'    commodities. This includes items such as offals, fats and hides and 
##'    impute missing values for non-meat commodities.
##'
##' **Flag assignment:**
##'
##' | Procedure | Observation Status Flag | Method Flag|
##' | --- | --- | --- |
##' | Tranasfer between animal and meat commodity | `<Same as origin>` | c |
##' | Balance by Production Identity | `<flag aggregation>` | i |
##' | Imputation | I | e |
##'
##' **NOTE (Michael): Currently the transfer has flag 'c' indicating it is
##' copied, however, they should be replaced with a new flag as it is calculated
##' by not by identity.**
##'
##' **Data scope**
##'
##' * GeographicAreaM49: All countries specified in the `Complete Key Table`.
##'
##' * measuredItemCPC: Depends on the session selection. If the selection is
##'   "session", then only items selected in the session will be imputed. If the
##'   selection is "all", then all the items listed in the `Livestock Element
##'   Mapping Table` will be imputed.
##'
##' * measuredElement: Depends on the measuredItemCPC, all cooresponding
##'   elements in the `Identity Formula Table` and also all elements listed in
##'   the `Livestock Element Mapping Table`.
##'
##' * timePointYears: All years specified in the `Complete Key Table`.
##'
##'
##' **Flow chart:**
##' ![livestock Flow](livestock_flow.jpg?raw=true "livestock Flow")
##' ---

##' ## Initialisation
##'

message("Step 0: Setup")

##' Load the libraries
suppressMessages({
    library(data.table)
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


completeImputationKey@dimensions$timePointYears@keys=c("2000" ,"2001" ,"2002" ,"2003" ,"2004" ,
                                                       "2005", "2006", "2007" ,"2008" ,"2009" ,
                                                       "2010" ,"2011", "2012" ,"2013","2014")

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
                         onlyMeatChildren = FALSE) 
animalMeatMappingTable= animalMeatMappingTable[,.(measuredItemParentCPC, measuredElementParent,
           measuredItemChildCPC, measuredElementChild)]

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


##' ---
##' ## Perform Synchronisation and Imputation

##' Here we iterate through the the meat item to perform the steps described in
##' the description. Essentially, we are looping over different livestock trees.



logConsole1=file("log.txt",open = "w")
sink(file = logConsole1, append = TRUE, type = c( "message"))

for(iter in seq(selectedMeatCode)){
    message("Processing livestock tree (", iter, " out of ",
            length(selectedMeatCode), ")")
    
    
    set.seed(070416)
  ## Extact the current ANIMAL,MEAT and NON-MEATcodes with their relative formula and mapping table
    
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
    message("\tExtracting animal data ", currentAnimalItem,
            " (Animal)")
    
    
    
    ## Get the animal formula
    animalFormulaTable =
        getProductionFormula(itemCode = currentAnimalItem) %>%
        removeIndigenousBiologicalMeat(formula = .)
    
    if(nrow(animalFormulaTable) > 1){
        stop("Imputation should only use one formula")}
    
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
    
    ## Francesca: it is not necessary to extract the triplet, but just Livestock and
    ## Slaughtered, the element that should play the role of the YIEL is, in this case 
    ## the take off rate that is endogenously computed (using also trade) and then imputed.
    animalKey = completeImputationKey
    animalKey@dimensions$measuredItemCPC@keys = currentAnimalItem
    animalKey@dimensions$measuredElement@keys =
        with(animalFormulaParameters,
             c(productionCode, areaHarvestedCode))
    
    ## Get the animal data
    animalData =
        animalKey %>%
        GetData(key = .) %>%
        preProcessing(data = .) %>%
        removeNonProtectedFlag(.)   
    
   
 

##    ## ---------------------------------------------------------------------  
##  The idea is to include the TRADE domain into the livestock imputation process. The basic hypothesis 
##  is that Countries import livestock just for slaughtering purposes.
##  We made several test including and excluding trade data which in many cases was the source of outliers 
##  apparently non feasible fluctuations into the meat production.  
##    
##    ########### Harvest from Trade: old FAOSTAT data
##    #### CRISTINA MODIFIED 23/05/2017
##    ##message("Pulling data from Trade")
##    
##    eleTradeDim = Dimension(name = "measuredElementTrade",
##                            keys = c("5608","5609"))
##    
##    tradeItems <- na.omit(sub("^0+", "", cpc2fcl(unique(currentAnimalItem), returnFirst = TRUE, version = "latest")))
##    
##    geoKeysTrade=m492fs(completeImputationKey@dimensions$geographicAreaM49@keys)
##    
##    geokeysTrade=geoKeysTrade[!is.na(geoKeysTrade)]
##    
##    tradeKey = DatasetKey(
##     domain = "faostat_one", dataset = "updated_sua",
##     dimensions = list(
##       #user input except curacao,  saint martin and former germany
##       geographicAreaFS= Dimension(name = "geographicAreaFS", keys = setdiff(geokeysTrade, c("279", "534", "280","274","283"))),
##       measuredItemFS=Dimension(name = "measuredItemFS", keys = tradeItems),
##       measuredElementFS=Dimension(name = "measuredElementFS",
##                 keys = c( "61", "91")),
##       timePointYears = Dimension(name = "timePointYears", keys = completeImputationKey@dimensions$timePointYears@keys) ),
##     sessionId =  slot(swsContext.datasets[[1]], "sessionId")
##    )
##    
##    
##    tradeData = GetData(tradeKey)
##    
##    
##    tradeData[, `:=`(geographicAreaFS = fs2m49(geographicAreaFS),
##                     measuredItemFS = fcl2cpc(sprintf("%04d", as.numeric(measuredItemFS)),
##                                              version = "latest"))]
##     
##     
##     setnames(tradeData, c("geographicAreaFS","measuredItemFS","measuredElementFS","flagFaostat" ),
##             c("geographicAreaM49", "measuredItemCPC","measuredElement","flagObservationStatus"))
##     
##     tradeData[, flagMethod := "-"]
##     
##     tradeData[flagObservationStatus %in% c("P", "*", "F"), flagObservationStatus := "T"]
##     
##     
##     
##     
##     tradeData[measuredElement=="91",measuredElement:="5909"]
##     tradeData[measuredElement=="61",measuredElement:="5609"]
##    #### End import FAOSTAT DATA for TRADE (CRISTINA)
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
##  We tried the both new and old TRADE data in order to exclude that the source of outliers was due
##  to the new and not yet validated TRADE domain  
##    ##Get new trade data
##    
   itemMap = GetCodeList(domain = "agriculture", dataset = "aproduction", "measuredItemCPC")
   itemMap=itemMap[,.(code,type)]
   setnames(itemMap, "code", "measuredItemCPC")
   data=merge(animalData, itemMap, by="measuredItemCPC")     
   
   
   
   itemCodeKey = ReadDatatable("element_codes")
   tradeElements=itemCodeKey[itemtype== unique(data[,type]),c(import, export)]
   ##Pull trade data for the current Animal Item    
##   
##   GetCodeList2 <- function(dimension = NA) {
##       GetCodeList(domain='trade', dataset='total_trade_cpc_m49', dimension = dimension)
##   }
##   
##   
##   Vars <- list(reporters = 'geographicAreaM49',
##                items     = 'measuredItemCPC',
##                elements  = 'measuredElementTrade',
##                years     = 'timePointYears')
##   
##   Keys <- list(reporters = GetCodeList2(dimension = Vars[['reporters']])[type=='country', code],
##                items     = c(currentAnimalItem),
##                # Quantity [#], Quantity [head], Quantity [1000 head], Quantity [t], Value [1000 $]
##                elements  = as.character(tradeElements),
##                years     = as.character(2000:2014))
##   
##   key <- DatasetKey(domain = 'trade',
##                     dataset = 'total_trade_cpc_m49',
##                     dimensions = list(
##                         Dimension(name = Vars[['reporters']], keys = Keys[['reporters']]),
##                         Dimension(name = Vars[['items']],     keys = Keys[['items']]),
##                         Dimension(name = Vars[['elements']],  keys = Keys[['elements']]),
##                         Dimension(name = Vars[['years']],     keys = Keys[['years']])))
##  
##   tradeData <- GetData(key = key)
##   
##   setnames(tradeData, c("measuredElementTrade", "measuredItemCPC"),
##            c("measuredElement", "measuredItemCPC"))
##   
##
##   tradeData=preProcessing(tradeData)
##   
##   
##   stockTrade=rbind(tradeData, animalData)
    
    stockTrade=animalData
    
    stockTrade=denormalise(stockTrade, denormaliseKey = "measuredElement", fillEmptyRecords=TRUE )
    
    ## ---------------------------------------------------------------------  
    
    
    ##Imputation of animal Stock
    
    ## To impute livestock numbers we follow excatly the same approach (the ensemble approach)
    ## already developped. Here we are building the parameters
    animalStockImputationParameters=defaultImputationParameters()
    

    ## I am modifing the animalStockImputationParameters in order to specify that the variable to be imputed
    ## is the livestock (5111 for big animals, 5112 for small animals)
    animalStockImputationParameters$imputationValueColumn=animalFormulaParameters$productionValue
    animalStockImputationParameters$imputationFlagColumn=animalFormulaParameters$productionObservationFlag
    animalStockImputationParameters$imputationMethodColumn=animalFormulaParameters$productionMethodFlag
    animalStockImputationParameters$byKey=c("geographicAreaM49","measuredItemCPC")
    animalStockImputationParameters$estimateNoData=TRUE
    ## I am  modifing the animalStockImputationParameters in order to exclude some modeles 
    ## from the ensemble approach
    animalStockImputationParameters$ensembleModels$defaultExp=NULL
    animalStockImputationParameters$ensembleModels$defaultLogistic=NULL
    animalStockImputationParameters$ensembleModels$defaultLoess=NULL
    animalStockImputationParameters$ensembleModels$defaultSpline=NULL
    animalStockImputationParameters$ensembleModels$defaultMars=NULL
    
    ##This code is to see the charts of the emsemble approach
    ##animalStockImputationParameters$plotImputation="prompt"
    
    message("\tStep 1: Impute missing values for livestock: item", currentAnimalItem,
            " (Animal)")
    
    
    stockTrade=imputeVariable(stockTrade,
                              imputationParameters = animalStockImputationParameters)	
    

    message("\tStep 2: Impute Number of Slaughtered animal for", currentAnimalItem,
            " (Animal)")
    
    slaughteredParentData=computeTotSlaughtered(data = stockTrade, tradeElements, FormulaParameters=animalFormulaParameters, plot=FALSE)
    
    
    ## ---------------------------------------------------------------------
    message("\tExtracting production triplet for item ", currentMeatItem,
            " (Meat)")
    ## Get the meat formula
    meatFormulaTable =
        getProductionFormula(itemCode = currentMeatItem) %>%
        removeIndigenousBiologicalMeat(formula = .)
    
    ## NOTE (Michael): Imputation should be performed on only 1 formula, if
    ##                 there are multiple formulas, they should be calculated
    ##                 based on the values imputed. For example, if one of thec
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
        GetData(key = .) %>%
        preProcessing(data = .) 
    

        meatData = denormalise(normalisedData = meatData,
                    denormaliseKey = "measuredElement") %>%
        createTriplet(data = .,
                      formula = meatFormulaTable)%>%
        processProductionDomain(data  = .,
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
                                   imputationEstimationMethodFlag = "f ") %>%
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
    
    
##################################################################################################################    
   
 ##  if(length(currentNonMeatItem) > 0){
 ##      
 ##      ## NOTE (Michael): We need to test whether the commodity has non-meat
 ##      ##                 item. For example, the commodity "Other Rodent"
 ##      ##                 (02192.01) does not have non-meat derived products
 ##      ##                 and thus we do not need to perform the action.
 ##      
 ##      message("\tExtracting production triplet for item ",
 ##              paste0(currentNonMeatItem, collapse = ", "),
 ##              " (Non-meat Child)")
 ##      ## Get the non Meat formula
 ##      nonMeatFormulaTable =
 ##          getProductionFormula(itemCode = currentNonMeatItem) %>%
 ##          removeIndigenousBiologicalMeat(formula = .)
 ##      
 ##      ## Build the non meat key
 ##      nonMeatKey = completeImputationKey
 ##      nonMeatKey@dimensions$measuredItemCPC@keys = currentNonMeatItem
 ##      nonMeatKey@dimensions$measuredElement@keys =
 ##          with(nonMeatFormulaTable,
 ##               unique(c(input, output, productivity,
 ##                        currentMappingTable$measuredElementChild)))
 ##      
 ##      ## Get the non meat data
 ##      ##
 ##      ## HACK (Michael): Current we don't test the input of non-meat item.
 ##      ##                 This is because the processProductionDomain and
 ##      ##                 ensureProductionInputs only work for triplets of a
 ##      ##                 single item. However, in the non-meat data, there are
 ##      ##                 more than one item and thus we are unable to process
 ##      ##                 and test them.
 ##      nonMeatData =
 ##          nonMeatKey %>%
 ##          GetData(key = .) %>%
 ##          preProcessing(data = .) %>%
 ##          denormalise(normalisedData = .,
 ##                      denormaliseKey = "measuredElement") %>%
 ##          createTriplet(data = .,
 ##                        formula = nonMeatFormulaTable) %>%
 ##          normalise(denormalisedData = .,
 ##                    removeNonExistingRecords = FALSE)
 ##      
 ##  }
    
    ## ---------------------------------------------------------------------
    message("\tStep 3: Transferring animal slaughtered from animal to meat commodity")
    animalMeatMappingShare =
        merge(currentMappingTable, shareData, all.x = TRUE,
              by = c("measuredItemParentCPC", "measuredItemChildCPC"))
    
    ## Transfer the animal slaughtered number from animal to the meat.
    slaughteredTransferedToMeatData =
        transferParentToChild(parentData = slaughteredParentData,
                              childData = meatData,
                              mappingTable = animalMeatMappingShare,
                              transferMethodFlag="c",
                              imputationObservationFlag = "I",
                              parentToChild = TRUE)
    
    ## ---------------------------------------------------------------------
    message("Step 4: Perform Imputation on the Meat Triplet")
    
    ## Start the imputation
    ## Build imputation parameter
    imputationParameters =
        with(meatFormulaParameters,
             getImputationParameters(productionCode = productionCode,
                                     areaHarvestedCode = areaHarvestedCode,
                                     yieldCode = yieldCode)
        )
    
    ##In the livestock module yield is represented by carcass weight
    ##that is a 
    imputationParameters$yieldParams$ensembleModels$defaultExp=NULL
    imputationParameters$yieldParams$ensembleModels$defaultLogistic=NULL
    imputationParameters$yieldParams$ensembleModels$defaultLoess=NULL
    imputationParameters$yieldParams$ensembleModels$defaultSpline=NULL
    imputationParameters$yieldParams$ensembleModels$defaultMars=NULL
    
    
    imputationParameters$areaHarvestedParams$ensembleModels$defaultExp=NULL
    imputationParameters$areaHarvestedParams$ensembleModels$defaultLogistic=NULL
    imputationParameters$areaHarvestedParams$ensembleModels$defaultLoess=NULL
    imputationParameters$areaHarvestedParams$ensembleModels$defaultSpline=NULL
    imputationParameters$areaHarvestedParams$ensembleModels$defaultMars=NULL
    ##imputationParameters$areaHarvestedParams$plotImputation="prompt"
    
    ## Perform imputation using the standard imputation function
    ##
    message("\tPerforming Imputation")
    
    meatImputed =
        slaughteredTransferedToMeatData

        meatImputed = denormalise(normalisedData = meatImputed,
                    denormaliseKey = "measuredElement",
                    fillEmptyRecord = TRUE)
        meatImputed =processProductionDomain(data = meatImputed,
                                processingParameters = processingParameters,
                                formulaParameters = meatFormulaParameters) 
        
        
        
## --------------------------------------------------------------------- 
        
        ##I highlighted the slaughtered animals computed by the "new" approach with SSS
        ##
        
        ##meatImputed[get(meatFormulaParameters$areaHarvestedObservationFlag)=="SSS",meatFormulaParameters$areaHarvestedObservationFlag:=""]
        
        rangeCarcassWeight=ReadDatatable("range_carcass_weight")
        
        
        meatFormulaParameters=c(meatFormulaParameters,list(areaHarvestedFlagComb=paste0("flagComb", meatFormulaParameters$areaHarvestedCode),
                                                           productionFlagComb=paste0("flagComb", meatFormulaParameters$productionCode),
                                                           yieldFlagComb=paste0("flagComb", meatFormulaParameters$yieldCode)))
        
        ##Obtain a vector containing all the protected flag combinations
        ProtectedFlag = faoswsFlag::flagValidTable[Protected == TRUE,]
        ProtectedFlag= ProtectedFlag[, combination := paste(flagObservationStatus, flagMethod, sep = ";")]
        ProtectedFlagComb=ProtectedFlag[,combination]
        
        
        ##Add the flag combination column for each element of the triplet
        
        meatImputed[,meatFormulaParameters$areaHarvestedFlagComb:=paste(get(meatFormulaParameters$areaHarvestedObservationFlag),get(meatFormulaParameters$areaHarvestedMethodFlag), sep=";")]
        meatImputed[,meatFormulaParameters$productionFlagComb:=paste(get(meatFormulaParameters$productionObservationFlag),get(meatFormulaParameters$productionMethodFlag), sep=";")]
        meatImputed[,meatFormulaParameters$yieldFlagComb:=paste(get(meatFormulaParameters$yieldObservationFlag),get(meatFormulaParameters$yieldMethodFlag), sep=";")]
        
        
        ##For those items for which there are more that one element protected we can anticipate the compilation of the carcass weight 
        ##and  check whether it is within a feasible range, if not some corretive procedures have been created
        
        
        meatImputed[, yield:=(get(meatFormulaParameters$productionValue)/get(meatFormulaParameters$areaHarvestedValue))*itemCodeKey[itemType== unique(data[,type]),c(factor)]]
         
       
        ##If the three elements of the triplet are all protected and the Carcass weight is not equal
        ##to meatProduction/ carcass weight, I have to free one variable.
        
         meatImputed[get(meatFormulaParameters$productionFlagComb) %in% ProtectedFlagComb &
                    get(meatFormulaParameters$areaHarvestedFlagComb) %in% ProtectedFlagComb &
                    get(meatFormulaParameters$yieldFlagComb) %in% ProtectedFlagComb &
                    (get(meatFormulaParameters$yieldValue) < yield-0.1 |get(meatFormulaParameters$yieldValue) > yield+0.1),":="(
                    c(meatFormulaParameters$areaHarvestedValue,meatFormulaParameters$areaHarvestedObservationFlag,meatFormulaParameters$areaHarvestedMethodFlag),
                    list(NA,"M","u"))]

        ## Where do negative values come from?
         
        meatImputed[get(meatFormulaParameters$areaHarvestedValue)<0,":="(
                            c(meatFormulaParameters$areaHarvestedValue,meatFormulaParameters$areaHarvestedObservationFlag,meatFormulaParameters$areaHarvestedMethodFlag),
                            list(NA,"M","u"))]   
        
        ## Free the number of animal slaughtered in order to remove the values of carcass weight out of the feasible range
        ## This part should be improved thanks to more accurate and region-specific carcass weights.
        
        
        
        
        meatImputed[yield> rangeCarcassWeight[meat_item_cpc==currentMeatItem, carcass_weight_max] |
                    yield< rangeCarcassWeight[meat_item_cpc==currentMeatItem, carcass_weight_max] ,":="(
                        c(meatFormulaParameters$areaHarvestedValue,meatFormulaParameters$areaHarvestedObservationFlag,meatFormulaParameters$areaHarvestedMethodFlag),
                            list(NA,"M","u"))]        
        
        
        meatImputed[,paste0("flagComb", meatFormulaParameters$areaHarvestedCode):=NULL]
        meatImputed[,paste0("flagComb", meatFormulaParameters$yieldCode):=NULL]
        meatImputed[,paste0("flagComb", meatFormulaParameters$productionCode):=NULL]
        meatImputed[,yield:=NULL]
        
        
       ## meatImputed[meatFormulaParameters$areaHarvestedValue<0, meatFormulaParameters$areaHarvestedObservationFlag:="M"]
       ## meatImputed[meatFormulaParameters$areaHarvestedValue<0, meatFormulaParameters$areaHarvestedMethodFlag:="u"]
       ## meatImputed[meatFormulaParameters$areaHarvestedValue<0, meatFormulaParameters$areaHarvestedValue:=NA]
## ---------------------------------------------------------------------    
        
        
        
      meatImputed = imputeProductionTriplet(data = meatImputed,
                                processingParameters = processingParameters,
                                imputationParameters = imputationParameters,
                                formulaParameters = meatFormulaParameters) 
      ensureProductionOutputs(data = meatImputed,
                                processingParameters = processingParameters,
                                formulaParameters = meatFormulaParameters,
                                returnData = FALSE,
                                normalised = FALSE)
      meatImputed =  normalise(meatImputed)
    
    
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
        transferParentToChild(parentData = slaughteredParentData,
                              childData = .,
                              mappingTable = animalMeatMappingShare,
                              transferMethodFlag="c",
                              imputationObservationFlag = "I",
                              parentToChild = FALSE)
       ##ensureProductionOutputs(data = .,
       ##                        processingParameters = processingParameters,
       ##                        formulaParameters = animalFormulaParameters,
       ##                        testImputed = FALSE,
       ##                        testCalculated = FALSE)
    
    ## ---------------------------------------------------------------------
    
 ##  if(length(currentNonMeatItem) > 0){
 ##      
 ##      ## NOTE (Michael): We need to test whether the commodity has non-meat
 ##      ##                 item. For example, the commodity "Other Rodent"
 ##      ##                 (02192.01) does not have non-meat derived products
 ##      ##                 and thus we do not need to perform the action.
 ##      message("Step 4: Transfer Animal Slaughtered to All Child Commodities")
 ##      
 ##      nonMeatMappingTable =
 ##          animalMeatMappingTable[measuredItemChildCPC %in% currentNonMeatItem, ]
 ##      
 ##      animalNonMeatMappingShare =
 ##          merge(nonMeatMappingTable, shareData, all.x = TRUE,
 ##                by = c("measuredItemParentCPC", "measuredItemChildCPC"))
 ##      
 ##      slaughteredTransferToNonMeatChildData =
 ##          transferParentToChild(parentData = slaughteredTransferedBackToAnimalData,
 ##                                childData = nonMeatData,
 ##                                transferMethodFlag="c",
 ##                                mappingTable = animalNonMeatMappingShare,
 ##                                parentToChild = TRUE)
 ##  }
    
    if(length(currentNonMeatItem) > 0){
        nonMeatImputedList=list()
        
        
        message("/tStep 6: Transfer the slaughtered animal from the animal to all other child
                 commodities. This includes items such as offals, fats and hides and 
                 impute missing values for non-meat commodities.")
        
        
        for(j in c(1:length(currentNonMeatItem))){
            currentNonMeatItemLoop= currentNonMeatItem[j]
            
            ## NOTE (Michael): We need to test whether the commodity has non-meat
            ##                 item. For example, the commodity "Other Rodent"
            ##                 (02192.01) does not have non-meat derived products
            ##                 and thus we do not need to perform the action.
            
            message("\tExtracting production triplet for item ",
                    paste0(currentNonMeatItem, collapse = ", "),
                    " (Non-meat Child)")
            ## Get the non Meat formula
            currentNonMeatFormulaTable =
                getProductionFormula(itemCode = currentNonMeatItemLoop) %>%
                removeIndigenousBiologicalMeat(formula = .)
            
            ## Build the non meat key
            currentNonMeatKey = completeImputationKey
            currentNonMeatKey@dimensions$measuredItemCPC@keys = currentNonMeatItemLoop
            currentNonMeatKey@dimensions$measuredElement@keys =
                with(currentNonMeatFormulaTable,
                     unique(c(input, output, productivity)))
            
            ## Get the non meat data
            ##
            ## HACK (Michael): Current we don't test the input of non-meat item.
            ##                 This is because the processProductionDomain and
            ##                 ensureProductionInputs only work for triplets of a
            ##                 single item. However, in the non-meat data, there are
            ##                 more than one item and thus we are unable to process
            ##                 and test them.
            nonMeatData =
                currentNonMeatKey %>%
                GetData(key = .) %>%
                preProcessing(data = .) %>%
                denormalise(normalisedData = .,
                            denormaliseKey = "measuredElement") %>%
                createTriplet(data = .,
                              formula = currentNonMeatFormulaTable) %>%
                normalise(denormalisedData = .,
                          removeNonExistingRecords = FALSE)
            
            
            
            ## NOTE (Michael): We need to test whether the commodity has non-meat
            ##                 item. For example, the commodity "Other Rodent"
            ##                 (02192.01) does not have non-meat derived products
            ##                 and thus we do not need to perform the action.
            message("Transfer Animal Slaughtered to All Child Commodities")
            
            nonMeatMappingTable =
                animalMeatMappingTable[measuredItemChildCPC %in% currentNonMeatItemLoop, ]
            
            animalNonMeatMappingShare =
                merge(nonMeatMappingTable, shareData, all.x = TRUE,
                      by = c("measuredItemParentCPC", "measuredItemChildCPC"))
            
            slaughteredTransferToNonMeatChildData =
                transferParentToChild(parentData = slaughteredTransferedBackToAnimalData,
                                      childData = nonMeatData,
                                      transferMethodFlag="c",
                                      imputationObservationFlag = "I",
                                      mappingTable = animalNonMeatMappingShare,
                                      parentToChild = TRUE)
            
            ## Imputation without removing the non protected figures					
            
            
            nonMeatImputationParameters=		
                with(currentNonMeatFormulaTable,
                     getImputationParameters(productionCode = output,
                                             areaHarvestedCode = input,
                                             yieldCode = productivity)
                )
            
            
            nonMeatMeatFormulaParameters =
                with(currentNonMeatFormulaTable,
                     productionFormulaParameters(datasetConfig = datasetConfig,
                                                 productionCode = output,
                                                 areaHarvestedCode = input,
                                                 yieldCode = productivity,
                                                 unitConversion = unitConversion)
                )
            
            slaughteredTransferToNonMeatChildData=denormalise(slaughteredTransferToNonMeatChildData, denormalise="measuredElement",fillEmptyRecords=TRUE )
            
            nonMeatImputed = imputeProductionTriplet(data = slaughteredTransferToNonMeatChildData,
                                                     processingParameters = processingParameters,
                                                     imputationParameters = nonMeatImputationParameters,
                                                     formulaParameters = nonMeatMeatFormulaParameters) 				
            
            nonMeatImputedList[[j]] = normalise(nonMeatImputed)
        }
    }
    
    slaughteredTransferToNonMeatChildData=rbindlist(nonMeatImputedList)
    ## ---------------------------------------------------------------------
    ##message("\tTesting transfers are applied correctly")
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
    ##ensureCorrectTransfer(parentData = slaughteredTransferedBackToAnimalData,
    ##                      childData = meatImputed,
    ##                      mappingTable = animalMeatMappingShare,
    ##                      returnData = FALSE)
    stockTrade=normalise(stockTrade)
    stockTrade=stockTrade[measuredElement==animalFormulaParameters$productionCode,]
    
    message("\tSaving the synchronised and imputed data back")
    if(length(currentNonMeatItem) > 0){
        syncedData = rbind(meatImputed,
                           stockTrade,
                           slaughteredTransferedBackToAnimalData,
                           slaughteredTransferToNonMeatChildData)
    } else {
        syncedData = rbind(meatImputed,
                           stockTrade,
                           slaughteredTransferedBackToAnimalData)
    }
    

    syncedData=syncedData[flagObservationStatus!="M" & flagMethod!="u",]
  ##  write.csv(syncedData, paste0("C:/Users/Rosa/Desktop/LIVETOCK NEW/FinalSyncedData/syncedData/",currentMeatItem,".csv"), row.names = FALSE)
    
    syncedData %>%
        ## NOTE (Michael): The transfer can over-write official and
        ##                 semi-official figures in the processed commodities as
        ##                 indicated by in the previous synchronise slaughtered
        ##                 module.
        ##
        ## NOTE (Michael): Records containing invalid dates are excluded, for
        ##                 example, South Sudan only came into existence in 2011.
        ##                 Thus although we can impute it, they should not be saved
        ##                 back to the database.
        removeInvalidDates(data = ., context = sessionKey) %>%
        postProcessing %>%
        SaveData(domain = sessionKey@domain,
                 dataset = sessionKey@dataset,
                 data = .)
    
    
    ## --------------------------------------------------------------------- 
    #' Check if the resulting Carcass weights are within a feasible range!
    #' We are currently use the 
    
    
    currentRange=rangeCarcassWeight[meat_item_cpc==currentMeatItem,] 
    
    meatData=meatImputed[!is.na(Value),]
    
    
    meatData=denormalise(meatData, denormaliseKey = "measuredElement")
    
    meatData=meatData[!is.na(get(imputationParameters$yieldParams$imputationValueColumn))
                      & !is.na(get(imputationParameters$productionParams$imputationValueColumn))
                      & !is.na(get(imputationParameters$areaHarvestedParams$imputationValueColumn))
                      
                      ,]
    
    outOfRange=meatData[get(imputationParameters$yieldParams$imputationValueColumn) >  currentRange[,carcass_weight_min] |
                            get(imputationParameters$yieldParams$imputationValueColumn) <  currentRange[,carcass_weight_min],]
    
    numberOfOutOfRange= dim(outOfRange)
    
    message("Number of rows ot of range: ", numberOfOutOfRange[1])
    
    
    
    
    ## Replace the values of carcass weight outside from the range with the extremes of the range
    
    
    
    ## Impose the outOfRange values below the minimum equal to the
    ## lower extreme of the range and  the outOfRange Values up the max equal to upper extreme of the range
    
    outOfRange[get(imputationParameters$yieldParams$imputationValueColumn) >  currentRange[,carcass_weight_max]
               & get(imputationParameters$yieldParams$imputationFlagColumn) !="M",
               ":="(c(imputationParameters$yieldParams$imputationValueColumn), 
                    c(currentRange[,carcass_weight_max]))]
    
    outOfRange[get(imputationParameters$yieldParams$imputationValueColumn) <  currentRange[,carcass_weight_min]
               & get(imputationParameters$yieldParams$imputationFlagColumn) !="M",
               ":="(c(imputationParameters$yieldParams$imputationValueColumn), 
                    currentRange[,carcass_weight_min])]
    
    ##Adjust the Flags for the new carcass weights
    
    outOfRange[get(imputationParameters$yieldParams$imputationFlagColumn) !="M",
               ":="(c(imputationParameters$yieldParams$imputationFlagColumn), c("I"))]
    outOfRange[get(imputationParameters$yieldParams$imputationFlagColumn) !="M",
               ":="(c(imputationParameters$yieldParams$imputationMethodColumn), c("e"))]
    
    
    
    ## We should free the number of anumal slaughtered and recalculate this variable as identity
    
    ## Report the number of official slaughterd animal overwritten
    
    
    protectedAreaHarvestedOverwritten = dim(outOfRange[get(imputationParameters$areaHarvestedParams$imputationFlagColumn)==""
                                                       & get(imputationParameters$yieldParams$imputationValueColumn)!=0
                                                       & get(imputationParameters$yieldParams$imputationFlagColumn) !="M"
                                                       & get(imputationParameters$productionParams$imputationFlagColumn) !="M", ])
    
    
    
    
    message("Number of items with official slaughtered animals that will be overwritten:  ", protectedAreaHarvestedOverwritten[1])
    
    
    
    outOfRange[get(imputationParameters$yieldParams$imputationValueColumn)!=0
               & get(imputationParameters$productionParams$imputationValueColumn)!=0
               & get(imputationParameters$areaHarvested$imputationValueColumn)!=0
               & get(imputationParameters$yieldParams$imputationFlagColumn) !="M"
               & get(imputationParameters$productionParams$imputationFlagColumn) !="M",
               ":="(c(imputationParameters$areaHarvestedParams$imputationValueColumn),
                    c(computeRatio(get(imputationParameters$productionParams$imputationValueColumn),
                                   get(imputationParameters$yieldParams$imputationValueColumn))*1000
                    ))]
    
    
    ## Adjust the ObservationFlag for animal slaughterd
    
    outOfRange[get(imputationParameters$yieldParams$imputationValueColumn)!=0
               & get(imputationParameters$productionParams$imputationValueColumn)!=0
               & get(imputationParameters$areaHarvested$imputationValueColumn)!=0
               & get(imputationParameters$yieldParams$imputationFlagColumn) !="M"
               & get(imputationParameters$productionParams$imputationFlagColumn) !="M",
               ":="(c(imputationParameters$areaHarvestedParams$imputationFlagColumn),
                    aggregateObservationFlag(get(imputationParameters$yieldParams$imputationFlagColumn),
                                             get(imputationParameters$productionParams$imputationFlagColumn)) )]
    
    ## Adjust the methodFlag for animal slaughterd
    
    outOfRange[get(imputationParameters$yieldParams$imputationValueColumn)!=0
               & get(imputationParameters$productionParams$imputationValueColumn)!=0
               & get(imputationParameters$areaHarvested$imputationValueColumn)!=0
               & get(imputationParameters$yieldParams$imputationFlagColumn) !="M"
               & get(imputationParameters$yieldParams$imputationMethodColumn) !="u",
               ":="(c(imputationParameters$areaHarvestedParams$imputationMethodColumn), c("i") )]
    
    
    
    
    
    outOfRange %>%
        normalise(.) %>%
        SaveData(domain = sessionKey@domain,
                 dataset = sessionKey@dataset,
                 data = .)
    ## --------------------------------------------------------------------- 
    
    message("\nSynchronisation and Imputation Completed for\n",
            "Animal Parent: ", currentAnimalItem, "\n",
            "Meat Child: ", currentMeatItem, "\n",
            "Non-meat Child: ", paste0(currentNonMeatItem, collapse = ", "), "\n",
            rep("-", 80), "\n")
    
}

