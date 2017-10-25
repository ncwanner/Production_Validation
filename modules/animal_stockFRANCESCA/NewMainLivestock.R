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
    library(sendmailR)
    
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



imputationTimeWindow = swsContext.computationParams$imputation_timeWindow
if(!imputationTimeWindow %in% c("all", "lastThree"))
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


##completeImputationKey@dimensions$timePointYears@keys=c("2000" ,"2001" ,"2002" ,"2003" ,"2004" ,
##                                                       "2005", "2006", "2007" ,"2008" ,"2009" ,
##                                                       "2010" ,"2011", "2012" ,"2013","2014")

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

lastYear=max(as.numeric(completeImputationKey@dimensions$timePointYears@keys))
attachment=list()
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
                                         unitConversion = unitConversion))
    
    
    ## Get the animal key, we take the complete key and then modify the element
    ## and item dimension to extract the current meat item and it's
    ## corresponding elements.
    ##
    ## NOTE (Michael): We extract the triplet so that we can perform the check
    ##                 on whether the triplet are balanced already. Eventhough
    ##                 only the animal slaughtered element is transferred.
    
    ## Francesca: it is not necessary to extract the triplet, but just Livestock and
    ## Slaughtered, the element that should play the role of the YIEL is, in this case 
    ## the off-take  rate that is endogenously computed (eventually using trade) and then imputed.
    animalKey = completeImputationKey
    animalKey@dimensions$measuredItemCPC@keys = currentAnimalItem
    animalKey@dimensions$measuredElement@keys =
        with(animalFormulaParameters,
             c(productionCode))
    
    ## Get the animal data
    animalData =
        animalKey %>%
        GetData(key = .) %>%
        preProcessing(data = .) %>%
        removeNonProtectedFlag(.) %>%
        expandYear(data = .,
                   areaVar = processingParameters$areaVar,
                   elementVar = processingParameters$elementVar,
                   itemVar = processingParameters$itemVar,
                   valueVar = processingParameters$valueVar,
                   newYears=lastYear) 
      
    
   
 

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
   tradeElements=itemCodeKey[itemtype== unique(data[,type]),c(imports, exports)]
##  Pull trade data for the current Animal Item    
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
   
## At the moment it has been decided to NOT use trade data
    
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
  ## from the ensemble approach, this attempt seems to be less robust that the complete ensemble approach
  # animalStockImputationParameters$ensembleModels$defaultExp=NULL
  # animalStockImputationParameters$ensembleModels$defaultLogistic=NULL
  # animalStockImputationParameters$ensembleModels$defaultLoess=NULL
  # animalStockImputationParameters$ensembleModels$defaultSpline=NULL
  # animalStockImputationParameters$ensembleModels$defaultMars=NULL
 
    ##This code is to see the charts of the emsemble approach
    ##animalStockImputationParameters$plotImputation="prompt"
    
    message("Step 1: Impute missing values for livestock: item ", currentAnimalItem,
            " (Animal)")
    
    
    stockTradeImputed=imputeVariable(stockTrade, imputationParameters = animalStockImputationParameters)	
    
    ##---------------------------------------------------------------------------------------------------------
    
    ##Pull slaughtered Animail (code referrig to ANIMAL)
    slaughterdKey=animalKey
    slaughterdKey@dimensions$measuredElement@keys= with(animalFormulaParameters,c(areaHarvestedCode))
    
    slaughteredAnimalData =
        slaughterdKey %>%
        GetData(key = .) %>%
        preProcessing(data = .) %>%
        removeNonProtectedFlag(.) %>%
        expandYear(data = .,
                   areaVar = processingParameters$areaVar,
                   elementVar = processingParameters$elementVar,
                   itemVar = processingParameters$itemVar,
                   valueVar = processingParameters$valueVar,
                   newYears=lastYear) 
      
    
    slaughteredAnimalData=denormalise(slaughteredAnimalData, denormaliseKey = "measuredElement", fillEmptyRecords=TRUE)

    ##---------------------------------------------------------------------------------------------------------
    ##Prepare the table to be used to compute TOT slaughtered Animal
    
    stockTrade=merge(stockTradeImputed, slaughteredAnimalData, by=c( "geographicAreaM49", "measuredItemCPC", "timePointYears"))
    
    ##---------------------------------------------------------------------------------------------------------

    message("Step 2: Impute Number of Slaughtered animal for ", currentAnimalItem," (Animal)")
            
    
    slaughteredParentData=computeTotSlaughtered(data = stockTrade, tradeElements, FormulaParameters=animalFormulaParameters)
    
    
    ## ---------------------------------------------------------------------
    message("\tExtracting production triplet for item ", currentMeatItem,
            " (Meat)")
    ## Get the meat formula
    meatFormulaTable =
        getProductionFormula(itemCode = currentMeatItem) %>%
        removeIndigenousBiologicalMeat(formula = .)
    
   ##Associated to each commodity we MUST have just ONE formula
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
        removeNonProtectedFlag(.)

        meatData = denormalise(normalisedData = meatData,
                    denormaliseKey = "measuredElement") 
     
        ## We have to remove (M,-) from the carcass weight: since carcass weght is usually computed ad identity,
        ## it results inutial that it exists a last available protected value different from NA and when we perform
        ## the function expandYear we risk to block the whole time series. I replace all the (M,-) carcass wight with
        ## (M,-) the triplet will be sychronized by the imputeProductionTriplet function.
        
        meatData[get(meatFormulaParameters$yieldObservationFlag)==processingParameters$missingValueObservationFlag,
                 ":="(c(meatFormulaParameters$yieldMethodFlag),list(processingParameters$missingValueMethodFlag)) ]
        
        
        meatData= createTriplet(data = meatData,
                      formula = meatFormulaTable)
        meatData= processProductionDomain(data  = meatData,
                                processingParameters = processingParameters,
                                formulaParameters = meatFormulaParameters) 
        ## NOTE (Michael): The function name should be generalised, here we are
        ##                 actually removing animal slaughtered values that were
        ##                 previously copied from the animal (parent).
            meatData =removeCalculated(data = meatData,
                                       valueVar =
                                       meatFormulaParameters$areaHarvestedValue,
                                       observationFlagVar =
                                       meatFormulaParameters$areaHarvestedObservationFlag,
                                       methodFlagVar =
                                       meatFormulaParameters$areaHarvestedMethodFlag,
                                       calculatedMethodFlag = "c") 
##       ## NOTE (Michael): Here we are removing manual estimates of animal
##       ##                 slaughtered of the meat. If there is to be any manual
##       ##                 estimates (which there shouldn't), it should be
##       ##                 inputted in the animal slaughtered element of the
##       ##                 animal (parent) commodity.
##       removeImputationEstimation(data = .,
##                                  valueVar =
##                                      meatFormulaParameters$areaHarvestedValue,
##                                  observationFlagVar =
##                                      meatFormulaParameters$areaHarvestedObservationFlag,
##                                  methodFlagVar =
##                                      meatFormulaParameters$areaHarvestedMethodFlag,
##                                  imputationEstimationObservationFlag = "E",
##                                  imputationEstimationMethodFlag = "f ") %>%
##       ## NOTE (Michael): The function name should be generalised, here we are
##       ##                 actually removing values imported from the old
##       ##                 system. It was indicated that the values are old and
##       ##                 can be over-written.
##       removeCalculated(data = .,
##                        valueVar =
##                            meatFormulaParameters$areaHarvestedValue,
##                        observationFlagVar =
##                            meatFormulaParameters$areaHarvestedObservationFlag,
##                        methodFlagVar =
##                            meatFormulaParameters$areaHarvestedMethodFlag,
##                        calculatedMethodFlag = "-") %>%
##       ensureProductionInputs(data = .,
##                              processingParameters = processingParameters,
##                              formulaParameters = meatFormulaParameters,
##                              normalised = FALSE) %>%
##       normalise(denormalisedData = .,
##                 removeNonExistingRecords = FALSE)
    meatData=normalise(meatData)
    
    meatData=expandYear(data = meatData,
               areaVar = processingParameters$areaVar,
               elementVar = processingParameters$elementVar,
               itemVar = processingParameters$itemVar,
               valueVar = processingParameters$valueVar,
               newYears=lastYear) 

    ## ---------------------------------------------------------------------
    message("Step 3: Transferring animal slaughtered from animal to meat commodity")
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
    
    
    ensureCorrectTransfer(parentData = slaughteredParentData,
                                       childData = slaughteredTransferedToMeatData,
                                       mappingTable = animalMeatMappingShare,
                                       returnData = FALSE)
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
   #imputationParameters$yieldParams$ensembleModels$defaultExp=NULL
   #imputationParameters$yieldParams$ensembleModels$defaultLogistic=NULL
   #imputationParameters$yieldParams$ensembleModels$defaultLoess=NULL
   #imputationParameters$yieldParams$ensembleModels$defaultSpline=NULL
   #imputationParameters$yieldParams$ensembleModels$defaultMars=NULL
   #
   #
   #imputationParameters$areaHarvestedParams$ensembleModels$defaultExp=NULL
   #imputationParameters$areaHarvestedParams$ensembleModels$defaultLogistic=NULL
   #imputationParameters$areaHarvestedParams$ensembleModels$defaultLoess=NULL
   #imputationParameters$areaHarvestedParams$ensembleModels$defaultSpline=NULL
   #imputationParameters$areaHarvestedParams$ensembleModels$defaultMars=NULL
   ###imputationParameters$areaHarvestedParams$plotImputation="prompt"
    
   
    message("\tPerforming Imputation")
    
    meatImputed =
        slaughteredTransferedToMeatData

        meatImputed = denormalise(normalisedData = meatImputed,
                    denormaliseKey = "measuredElement",
                    fillEmptyRecord = TRUE)
        meatImputed =processProductionDomain(data = meatImputed,
                                processingParameters = processingParameters,
                                formulaParameters = meatFormulaParameters) 
 

## Since we have syncronized and protected "slaugtered animal"
## and we have protected some of the carcass weight copied from the old
## system in order to stabilize the imputation of this variable, it is possible that
## we have some all protected triplets and we have to check:
## 1. the three elements are balanced  
## 2. if only slaughtered and production are balanced, the resulting
##    carcass weight is within the ranges                
        
        
        rangeCarcassWeight=ReadDatatable("range_carcass_weight")
        currentRange=rangeCarcassWeight[meat_item_cpc==currentMeatItem,] 
        
        ## I add to the already existing formula parameters the flagComb columns because I have to work 
        ## with PROTECTED flag combinations
        
        ## Enlarge the meatFormulaParameters just to include Flag cheks:
        meatFormParams=c(meatFormulaParameters,list(areaHarvestedFlagComb=paste0("flagComb_", meatFormulaParameters$areaHarvestedCode),
                                                           productionFlagComb=paste0("flagComb_", meatFormulaParameters$productionCode),
                                                           yieldFlagComb=paste0("flagComb_", meatFormulaParameters$yieldCode)))
        
       
        
        ##Obtain a vector containing all the protected flag combinations
        ProtectedFlag = faoswsFlag::flagValidTable[Protected == TRUE,]
        
        ProtectedFlag= ProtectedFlag[, combination := combineFlag(ProtectedFlag,"flagObservationStatus", "flagMethod")]
        ##I have to exclude (M,-) from the protected flag combinations. Doing the checks for the carcass weight to 
        ##free, otherwise I risk to open closed series:
        ProtectedFlag=ProtectedFlag[combination!="(M, -)" ]
        
        ProtectedFlagComb=ProtectedFlag[,combination]
        
       
        
        ##Add the flag combination column for each element of the triplet
        meatImputed[,meatFormParams$areaHarvestedFlagComb:=combineFlag(meatImputed,meatFormParams$areaHarvestedObservationFlag,meatFormParams$areaHarvestedMethodFlag)]
        meatImputed[,meatFormParams$productionFlagComb:=combineFlag(meatImputed,meatFormParams$productionObservationFlag,meatFormParams$productionMethodFlag)]
        meatImputed[,meatFormParams$yieldFlagComb:=combineFlag(meatImputed,meatFormParams$yieldObservationFlag,meatFormParams$yieldMethodFlag)]
        
       
        
        
        ##For those items for which there are more that one protected element  we can anticipate the compilation of the carcass weight 
        ##and  check whether it is within a feasible range, if not some corretive procedures have been created
        
       
        
        meatImputed[, yield:=(get(meatFormParams$productionValue)/get(meatFormParams$areaHarvestedValue))*itemCodeKey[itemtype== unique(data[,type]),c(factor)]]
         
       
        ##If the three elements of the triplet are all protected and the Carcass weight is not equal
        ##to meatProduction/ carcass weight, I have to free one variable: in this situation I free the CARCASS WEIGHT 
        ##because SLAUGHTERD and PRODUCTION are PROTECTED
        
        allProtectedEl=meatImputed[,get(meatFormParams$productionFlagComb) %in% ProtectedFlagComb &
                                       get(meatFormParams$areaHarvestedFlagComb) %in% ProtectedFlagComb &
                                       get(meatFormParams$yieldFlagComb) %in% ProtectedFlagComb]
        
        
      ##  meatImputed[allProtectedEl &
      ##              (get(meatFormParams$yieldValue) < yield-0.5 |get(meatFormParams$yieldValue) > yield+0.5),":="(
      ##              c(meatFormParams$yieldValue,meatFormParams$yieldObservationFlag,meatFormParams$yieldMethodFlag),
      ##              list(NA,"M","u"))]
       
        
      meatImputed[allProtectedEl , ":="(
                  c(meatFormParams$yieldValue,meatFormParams$yieldObservationFlag,meatFormParams$yieldMethodFlag),
                  list(NA_real_,"M","u"))]
        

      ##If we uses the trade there were the possiblity to have some negative values:manage this kind of situation
      meatImputed[get(meatFormParams$areaHarvestedValue)<0,":="(
                            c(meatFormParams$areaHarvestedValue,meatFormParams$areaHarvestedObservationFlag,meatFormParams$areaHarvestedMethodFlag),
                            list(NA_real_,"M","u"))]   
        
        ## Free the number of animal slaughtered in order to remove the values of carcass weight out of the feasible range
        ## This part should be improved thanks to more accurate and region-specific carcass weights, at the momento a data.table contained in
        ## the SWS is used      
        ##It the resulting CARCASS WEIGHT is not with the range I do not have other possibility than free the SLAUGHTERED animal        
        ##First set of SLAUGHTERED figures that have to be checked:
        
        ##toBechecked1=meatImputed[yield> rangeCarcassWeight[meat_item_cpc==currentMeatItem, carcass_weight_max] |
        ##                            yield< rangeCarcassWeight[meat_item_cpc==currentMeatItem, carcass_weight_min] &
        ##                            flagObservationStatus_measuredElement_5320!="I" &flagObservationStatus_measuredElement_5320!="E" ,]
        
        ## If protected SLAUGHTER and protected PRODUCTION do not produce feasible CARASS WEIGHT I free SLAUGHTERED
        ## Here it is not possible to overwrite a figure flagged as (M,-) because in this case the yield would be NA:
        meatImputed[yield> currentRange[, carcass_weight_max] |
                    yield< currentRange[, carcass_weight_min] ,":="(
                        c(meatFormParams$areaHarvestedValue,meatFormParams$areaHarvestedObservationFlag,meatFormParams$areaHarvestedMethodFlag),
                            list(NA_real_,"M","u"))]        
        
        ##I remove the flagComb columns that  have created just to make these checks
        meatImputed[,meatFormParams$areaHarvestedFlagComb:=NULL]
        meatImputed[,meatFormParams$yieldFlagComb:=NULL]
        meatImputed[,meatFormParams$productionFlagComb:=NULL]
        meatImputed[,yield:=NULL]
        

## ---------------------------------------------------------------------    
## Perform imputation using the standard imputation function
        
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
      
    ## I am filtering meatImputed in order to avoid issue 180 
      
      meatImputedFilterd =
          meatImputed[flagMethod == "i" | (flagObservationStatus == "I" & flagMethod == "e"), ]
      
      
      slaughteredTransferedBackToAnimalData=  transferParentToChild(parentData = slaughteredParentData,
                                                                    childData = meatImputedFilterd,
                                                                    mappingTable = animalMeatMappingShare,
                                                                    transferMethodFlag="c",
                                                                    imputationObservationFlag = "I",
                                                                    parentToChild = FALSE)
      
       ensureProductionOutputs(data = meatImputed,
                               processingParameters = processingParameters,
                               formulaParameters = meatFormulaParameters,
                               testImputed = FALSE,
                               testCalculated = FALSE,
                               normalised = TRUE,
                               returnData = FALSE)
    
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
    ##                    related to issue 178.(SOLVED)
    ##
    
       
      ##Slaughtered trasfered back from meat item to animal are those that should be 
      ##checked. 
      ensureCorrectTransfer(parentData = slaughteredTransferedBackToAnimalData,
                          childData = meatImputed,
                          mappingTable = animalMeatMappingShare,
                          returnData = FALSE)
      
      
      slaughteredToBeChecked1=postProcessing(slaughteredTransferedBackToAnimalData)
      
      if(nrow(slaughteredToBeChecked1)>0){
      
      slaughteredToBeChecked1=removeInvalidDates(slaughteredToBeChecked1 ,context = sessionKey)
      slaughteredToBeChecked1=slaughteredToBeChecked1[flagMethod=="c"]
      slaughteredToBeChecked1=ensureProtectedData(slaughteredToBeChecked1, getInvalidData = TRUE)
      slaughteredToBeChecked1=slaughteredToBeChecked1[flagObservationStatus %in% c("", "T"),]
      }
      
      
    ## I have to send back to the SWS the following elements:
    ## 1.Livestock numebers stockTradeImputed (NB: the name of this dataset refers to the possibility to use trade data )
    ## 2.Slaughtered animal associated to ANIMAL (slaughteredTransferedBackToAnimalData) 
      
    ## 3.4.5. The meat triplet contained in meatImputed   
   
    livestockNumbers=normalise(stockTradeImputed)
    ##livestockNumbers=livestockNumbers[measuredElement==animalFormulaParameters$productionCode,]
    
    message("\tSaving the synchronised and imputed data back")

        syncedData = rbind(meatImputed,
                           livestockNumbers,
                           slaughteredTransferedBackToAnimalData
                           )
        
   ##Maybe it better to send back also the (M,u) series otherwise it seems they are not updated!
   syncedData=syncedData[(flagMethod!="u"),]
    ##write.csv(syncedData, paste0("C:/Users/Rosa/Desktop/LivestockFinalDebug/syncedData/",currentMeatItem,".csv"), row.names = FALSE)
    
    
        ## NOTE (Michael): The transfer can over-write official and
        ##                 semi-official figures in the processed commodities as
        ##                 indicated by in the previous synchronise slaughtered
        ##                 module.
        ##
        ## NOTE (Michael): Records containing invalid dates are excluded, for
        ##                 example, South Sudan only came into existence in 2011.
        ##                 Thus although we can impute it, they should not be saved
        ##                 back to the database.
        syncedData=  removeInvalidDates(data = syncedData, context = sessionKey)
        
        
        if(imputationTimeWindow=="lastThree")
        {
         
            syncedData=syncedData[get(processingParameters$yearVar) %in% c(lastYear, lastYear-1, lastYear-2)]
            
            syncedData= postProcessing(data =  syncedData) 
            SaveData(domain = sessionKey@domain,
                     dataset = sessionKey@dataset,
                     data = syncedData)
        }else{ 
        syncedData= postProcessing(data =  syncedData) 
        SaveData(domain = sessionKey@domain,
                 dataset = sessionKey@dataset,
                 data = syncedData)
        }
       # ## ---------------------------------------------------------------------         
       # sendOnCSVfile1=meatImputed
       # sendOnCSVfile1=meatImputed[flagObservationStatus=="M",]  
       # sendOnCSVfile1=sendOnCSVfile1[flagMethod!="-",]
       # sendOnCSVfile1=unique(sendOnCSVfile1[,.(geographicAreaM49,measuredItemCPC,measuredElement)])
       # 
       # write.csv(sendOnCSVfile1,paste0("C:\\Users\\Rosa\\Desktop\\livestock\\LivestockValidation\\emptyCell+yield not recomputed\\NonImputedSeries",currentMeatItem, ".csv"),row.name=TRUE)
        ## --------------------------------------------------------------------- 
        

        
    ## --------------------------------------------------------------------- 
    #' Check if the resulting Carcass weights are within a feasible range!
    #' We are currently use the table range stored in the SWS
    
    ##Select the row corresponding to the current meat item from the range-table
        
    message("\tCheck the Carcass weights")    
        
    ##currentRange=rangeCarcassWeight[meat_item_cpc==currentMeatItem,] 
    ## I am checking only those series where the Value is different from NA:
    ## it means that is cannot overwrite (M,-) figures in the carcass weigth series.
    meatData=meatImputed[!is.na(Value),]
    meatData=denormalise(meatData, denormaliseKey = "measuredElement")
    ##Select rows where the all the elements of the triplet are NOT NA

    meatData=meatData[!is.na(get(imputationParameters$yieldParams$imputationValueColumn))
                      & !is.na(get(imputationParameters$productionParams$imputationValueColumn))
                      & !is.na(get(imputationParameters$areaHarvestedParams$imputationValueColumn)) ,]
                                           
    ## Identify the rows out of range
    outOfRange=meatData[get(imputationParameters$yieldParams$imputationValueColumn) >  currentRange[,carcass_weight_max] |
                            get(imputationParameters$yieldParams$imputationValueColumn) <  currentRange[,carcass_weight_min],]
    
    
    ##I have to create this empty object that will be populated only if nrow(outOfRange) is greater that one!
    slaughteredToBeChecked2=data.table()
    
    
    if(nrow(outOfRange)>0){
    numberOfOutOfRange= dim(outOfRange)
    message("Number out rows out of range: ", numberOfOutOfRange[1])

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
    
    
    
    ## We should free the number of animal slaughtered and recalculate this variable as identity   
    
    
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
    
    
    outOfRange=  normalise(outOfRange)
    
    ## Now that I have updated some figures for Slaughtered animal I have to syncronize back to the livetsock numbers!!!
    slaughteredTransferedBackToAnimalData=  transferParentToChild(parentData = slaughteredTransferedBackToAnimalData,
                                 childData = outOfRange,
                                 mappingTable = animalMeatMappingShare,
                                 transferMethodFlag="c",
                                 imputationObservationFlag = "I",
                                 parentToChild = FALSE)
    
    
    ## After this step a few figures for slaughtered animal (5315-5316) have been updated and have to be 
    ## sent to the SWS. I have to sent back to the SWS only that values that have been updated and flagged with "c"
    
    ## This is not correct because I have to isolate 
    slaughteredTransferedBackToAnimalDataC=slaughteredTransferedBackToAnimalData[flagMethod == "c",]
    
    ##Prepare the file to be send by email
    slaughteredToBeChecked2=postProcessing(slaughteredTransferedBackToAnimalDataC)
    slaughteredToBeChecked2=removeInvalidDates(slaughteredToBeChecked2 ,context = sessionKey)
    slaughteredToBeChecked2=ensureProtectedData(slaughteredToBeChecked2, getInvalidData = TRUE)
    
    ##Select just those figures flagged as official, semiofficial
    slaughteredToBeChecked2=slaughteredToBeChecked2[flagObservationStatus %in% c("","T")]
    
    
    
    outOfRangeSync=rbind(slaughteredTransferedBackToAnimalDataC,outOfRange)
    outOfRangeSync = removeInvalidDates(data =outOfRangeSync, context = sessionKey)
    
    message("\tSend to the SWS the upfdated figures and their syncronization : ",
            currentMeatItem, "syncronized on", currentAnimalItem)
    
    
    if(imputationTimeWindow=="lastThree")
    {
        attachment[[iter]]=toBeChecked[get(processingParameters$yearVar) %in% c(lastYear, lastYear-1, lastYear-2),]
        outOfRangeSync=outOfRangeSync[get(processingParameters$yearVar) %in% c(lastYear, lastYear-1, lastYear-2)]
        
        outOfRangeSync= postProcessing(data =  outOfRangeSync) 
        
        SaveData(domain = sessionKey@domain,
                 dataset = sessionKey@dataset,
                 data = outOfRangeSync)
    }else{ 
        
        outOfRangeSync= postProcessing(data =  outOfRangeSync) 
        
        
        SaveData(domain = sessionKey@domain,
                 dataset = sessionKey@dataset,
                 data = outOfRangeSync)
    }
    
    
    }
    
 
    
    ## Create the attachment to be sent by email 
    
    if(nrow(slaughteredToBeChecked1)>0 & nrow(slaughteredToBeChecked2)>0){
    toBeChecked=rbind(slaughteredToBeChecked1, slaughteredToBeChecked2)
    attachment[[iter]]=toBeChecked
    }
    
    if(nrow(slaughteredToBeChecked1)>0){
        
        attachment[[iter]]=slaughteredToBeChecked1
    }
    
    if(nrow(slaughteredToBeChecked2)>0){
        
        attachment[[iter]]=slaughteredToBeChecked2
    }
    
    
    ## Now that we have computed and synchronized all the slaughtered we can proceed 
    ##computig other derivatives
    
 ## ---------------------------------------------------------------------    
   #sendOnCSVfile2=denormalise(syncedData, denormaliseKey = "measuredElement")
   #sendOnCSVfile2[,yieldTest:=(Value_measuredElement_5510/Value_measuredElement_5320)*1000]
   #sendOnCSVfile2=sendOnCSVfile2[yieldTest>Value_measuredElement_5417+0.5 | yieldTest<Value_measuredElement_5417-0.5 ]
   #write.csv(sendOnCSVfile2, paste0("C:\\Users\\Rosa\\Desktop\\livestock\\LivestockValidation\\emptyCell+yield not recomputed\\NonMAtchingYield",currentMeatItem, ".csv"),row.name=TRUE)
    
 ## ---------------------------------------------------------------------    
    if(length(currentNonMeatItem) > 0){
        nonMeatImputedList=list()
        
        
        message("Step 6: Transfer the slaughtered animal from the animal to all other child
                commodities. This includes items such as offals, fats and hides and 
                impute missing values for non-meat commodities.")
        
        
        for(j in seq(currentNonMeatItem)){
            currentNonMeatItemLoop= currentNonMeatItem[j]
            
            ## NOTE (Michael): We need to test whether the commodity has non-meat
            ##                 item. For example, the commodity "Other Rodent"
            ##                 (02192.01) does not have non-meat derived products
            ##                 and thus we do not need to perform the action.
            
            message("\tExtracting production triplet for item ",
                    paste0(currentNonMeatItemLoop, collapse = ", "),
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
            
            
            nonMeatMeatFormulaParameters =
                with(currentNonMeatFormulaTable,
                     productionFormulaParameters(datasetConfig = datasetConfig,
                                                 productionCode = output,
                                                 areaHarvestedCode = input,
                                                 yieldCode = productivity,
                                                 unitConversion = unitConversion)
                )
            
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
                              formula = currentNonMeatFormulaTable) 
            
            
            ## We have to remove (M,-) from the carcass weight: since carcass weght is usually computed ad identity,
            ## it results inutial that it exists a last available protected value different from NA and when we perform
            ## the function expandYear we risk to block the whole time series. I replace all the (M,-) carcass wight with
            ## (M,-) the triplet will be sychronized by the imputeProductionTriplet function.
            
            nonMeatData[get(nonMeatMeatFormulaParameters$yieldObservationFlag)==processingParameters$missingValueObservationFlag,
                     ":="(c(nonMeatMeatFormulaParameters$yieldMethodFlag),list(processingParameters$missingValueMethodFlag)) ]
            
            
            nonMeatData = normalise(denormalisedData =nonMeatData,
                          removeNonExistingRecords = FALSE)
            
            nonMeatData= expandYear(data = nonMeatData,
                                    areaVar = processingParameters$areaVar,
                                    elementVar = processingParameters$elementVar,
                                    itemVar = processingParameters$itemVar,
                                    valueVar = processingParameters$valueVar,
                                    newYears=lastYear)
           
            message("Transfer Animal Slaughtered to All Child Commodities")
            
            nonMeatMappingTable =
                animalMeatMappingTable[measuredItemChildCPC %in% currentNonMeatItemLoop, ]
            
            animalNonMeatMappingShare =
                merge(nonMeatMappingTable, shareData, all.x = TRUE,
                      by = c("measuredItemParentCPC", "measuredItemChildCPC"))
            
            
            ## Delete those figures previously           
            ## In this tipology of commodity are still present old FAOSTAT imputations flagged as (I,-).
            ## At the moment the best we can is to keep those figures as protected.
            ## We delete the figures flagged ad (I,e) coming from previus run of themodule:
            
            nonMeatData=removeImputationEstimation (nonMeatData, "Value", "flagObservationStatus", "flagMethod")
            nonMeatData=removeCalculated(nonMeatData, "Value", "flagObservationStatus", "flagMethod")
            
            ## Syncronize  slaughteredTransferedBackToAnimalData to the slaughtered element associated to the 
            ## non-meat item
            slaughteredTransferToNonMeatChildData =
                transferParentToChild(parentData = slaughteredTransferedBackToAnimalData,
                                      childData = nonMeatData,
                                      transferMethodFlag="c",
                                      imputationObservationFlag = "I",
                                      mappingTable = animalNonMeatMappingShare,
                                      parentToChild = TRUE)
            
            
            
            nonMeatImputationParameters=		
                with(currentNonMeatFormulaTable,
                     getImputationParameters(productionCode = output,
                                             areaHarvestedCode = input,
                                             yieldCode = productivity)
                )
            
            
            ## Imputation without removing all the non protected figures for Production and carcass weight!					
            
            
            ## Some checks are requested because we cannot remove all the non protected values. 
            ## 1. SLAUGHTERED: synchronized
            ## 2. YIELD: to stabilize imputations I have to keep non-protected figures 
            ## 3. Non-MEAT PRODUCTION: remove non-protected figures, computed as IDENTITY (where possible), IMPUTED 
            
            ##slaughteredTransferToNonMeatChildDataPROD=slaughteredTransferToNonMeatChildData[measuredElement==nonMeatMeatFormulaParameters$productionCode]
            ##slaughteredTransferToNonMeatChildDataNoPROD=slaughteredTransferToNonMeatChildData[(measuredElement!=nonMeatMeatFormulaParameters$productionCode)]
            ##Remove non protected flags just for PRODUCTION
            ##slaughteredTransferToNonMeatChildDataPROD =  removeNonProtectedFlag(slaughteredTransferToNonMeatChildDataPROD)
            ##slaughteredTransferToNonMeatChildData=rbind(slaughteredTransferToNonMeatChildDataNoPROD,slaughteredTransferToNonMeatChildDataPROD)
   
            slaughteredTransferToNonMeatChildData=denormalise(slaughteredTransferToNonMeatChildData, denormalise="measuredElement",fillEmptyRecords=TRUE )
            
            
            ## In addition, since the number of animal slaugheterd might have changed, we delete also  the 
            ## the figures previously calculated ad identity (flagMethod="i") if also production is available
            
            ##remove those yields where both PRODUCTION and SLAUGHTERED are not NA:
            
            noNAProd=slaughteredTransferToNonMeatChildData[,!is.na(get(nonMeatMeatFormulaParameters$productionValue))]
            noNASlaughterd=slaughteredTransferToNonMeatChildData[,!is.na(get(nonMeatMeatFormulaParameters$areaHarvestedValue))]
            filter= noNAProd & noNASlaughterd
            
            slaughteredTransferToNonMeatChildData[filter, ":="(c(nonMeatMeatFormulaParameters$yieldValue,
                                                                                            nonMeatMeatFormulaParameters$yieldObservationFlag,
                                                                                            nonMeatMeatFormulaParameters$yieldMethodFlag), list(NA_real_,"M","u"))]
            
 
            nonMeatImputed = imputeProductionTriplet(data = slaughteredTransferToNonMeatChildData,
                                                     processingParameters = processingParameters,
                                                     imputationParameters = nonMeatImputationParameters,
                                                     formulaParameters = nonMeatMeatFormulaParameters) 				
            
            nonMeatImputedList[[j]] = normalise(nonMeatImputed)
            
            slaughteredTransferToNonMeatChildData=rbindlist(nonMeatImputedList)
            
      
            slaughteredTransferToNonMeatChildData=slaughteredTransferToNonMeatChildData[flagMethod!="u", ]
        
        if(imputationTimeWindow=="lastThree")
        {
            
            slaughteredTransferToNonMeatChildData=slaughteredTransferToNonMeatChildData[get(processingParameters$yearVar) %in% c(lastYear, lastYear-1, lastYear-2)]
            slaughteredTransferToNonMeatChildData = removeInvalidDates(data =slaughteredTransferToNonMeatChildData, context = sessionKey)
            slaughteredTransferToNonMeatChildData= postProcessing(data =  slaughteredTransferToNonMeatChildData) 
            SaveData(domain = sessionKey@domain,
                     dataset = sessionKey@dataset,
                     data = slaughteredTransferToNonMeatChildData)
        }else{ 
            slaughteredTransferToNonMeatChildData= postProcessing(data =  slaughteredTransferToNonMeatChildData) 
            slaughteredTransferToNonMeatChildData = removeInvalidDates(data =slaughteredTransferToNonMeatChildData, context = sessionKey)
            slaughteredTransferToNonMeatChildData= postProcessing(data =  slaughteredTransferToNonMeatChildData) 
            SaveData(domain = sessionKey@domain,
                     dataset = sessionKey@dataset,
                     data = slaughteredTransferToNonMeatChildData)
        }
        
        
        
        
        
        }
        
    
    }
#
    ## --------------------------------------------------------------------- 
    
    message("\nSynchronisation and Imputation Completed for\n",
            "Animal Parent: ", currentAnimalItem, "\n",
            "Meat Child: ", currentMeatItem, "\n",
            "Non-meat Child: ", paste0(currentNonMeatItem, collapse = ", "), "\n",
            rep("-", 80), "\n")
    
}




if(!CheckDebug() & length(attachment)>0){
    
    
    attachment=rbindlist(attachment)
    createErrorAttachmentObject = function(testName,
                                           testResult,
                                           R_SWS_SHARE_PATH){
        errorAttachmentName = paste0(testName, ".csv")
        errorAttachmentPath =
            paste0(R_SWS_SHARE_PATH, "/rosa/", errorAttachmentName)
        write.csv(testResult, file = errorAttachmentPath,
                  row.names = FALSE)
        errorAttachmentObject = mime_part(x = errorAttachmentPath,
                                          name = errorAttachmentName)
        errorAttachmentObject
    }
    
    bodyWithAttachment=
        createErrorAttachmentObject("ToBeChecked",
                                    attachment,
                                    R_SWS_SHARE_PATH)
    
    
    
    
    sendmail(from = "sws@fao.org",
             to = swsContext.userEmail,
             subject = "Some protected figures have been overwritten",
             msg = bodyWithAttachment)
    
    
    
    
} else {
    msg = "Production Input Validation passed without any error!"
    message(msg)
    msg
}

msg = "Imputation Completed Successfully"