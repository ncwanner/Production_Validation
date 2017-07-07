#' MILK IMPUTATION Sub-module
#' 
#' milkItems=c("02211","02212","02291","02292","02293") which correspond to their relative animalItem 
#' c("02111","02112","02122","02123","02121.01"). The correspondence between the milk and animal item is
#' contained in the table animalMilkCorrespondence.RData file stored in the package. The module works with
#' items (both milk and animal) which share the same Elements.
#' Livestock= 5111 (heads)
#' Milking animals (heads) [input]
#' Milk Prod= 5510 (tons)  [output]
#' Yiels= 5417 (kg/heads)  [productivity]
#' 
#' Intead of using the ensemble apporach 
#' 1) estimate the dairy animals when it is missed 
#'
#' 

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
    library(bit64)
    library(curl)
    library(lme4)
    library(reshape2)
    library(igraph)
    library(plyr)
    library(ggplot2)
    library(splines)
    
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




##' Get data configuration and session
sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)

##' Build processing parameters
processingParameters =
    productionProcessingParameters(datasetConfig = datasetConfig)

##' Obtain the complete imputation key
completeImputationKey = getCompleteImputationKey("production")


animalMilkCorrespondence=rangeCarcassWeight=ReadDatatable("animal_milk_correspondence")


livestockItems=animalMilkCorrespondence[,animal_item_cpc]


livestockFormulaTable =
    getProductionFormula(itemCode = livestockItems[1])



livestockFormulaParameters =
    with(livestockFormulaTable,
         productionFormulaParameters(datasetConfig = datasetConfig,
                                     productionCode = output,
                                     areaHarvestedCode = input,
                                     yieldCode = productivity,
                                     unitConversion = unitConversion)
    )







##Pull livestock numbers

completeImputationKey@dimensions$measuredItemCPC@keys= livestockItems
completeImputationKey@dimensions$measuredElement@keys= livestockFormulaParameters$productionCode

livestockData=GetData(completeImputationKey)



## ---------------------------------------------------------------------  
##Pull milking animals

itemMap = GetCodeList(domain = "agriculture", dataset = "aproduction", "measuredItemCPC")
milkItems=itemMap[type=="MILK",code]


milkFormulaTable=
    getProductionFormula(itemCode = milkItems[1])

milkFormulaParameters =
    with(
        milkFormulaTable,
        productionFormulaParameters(
            datasetConfig = datasetConfig,
            productionCode = output,
            areaHarvestedCode = input,
            yieldCode = productivity,
            unitConversion = unitConversion
        )
    )





milkKey=completeImputationKey
milkKey@dimensions$measuredItemCPC@keys= milkItems
milkKey@dimensions$measuredElement@keys= milkFormulaParameters$areaHarvestedCode

milkingAnimalData=GetData(milkKey)

##remove non portected flag combinations for milking animals which is the variable that has to be imputed

milkingAnimalData= removeNonProtectedFlag(milkingAnimalData)

##Impute milking animals

milkingAnimalsDataToModel =rbind(livestockData, milkingAnimalData )

milkingAnimalsDataToModel=preProcessing(milkingAnimalsDataToModel)


#'
#'  In this data I have 2 different Typologies of commodities: 
#'  1) animals
#'  2) milk
#'  I have to convert the animal commoties into their correspondet milk commodities
#'  02211         Raw milk of cattle           02111    Cattle
#'  02212         Raw milk of buffalo          02112    Buffalo
#'  02291         Raw milk of sheep            02122    Sheep
#'  02292         Raw milk of goats            02123    Goats
#'  02293         Raw milk of camel            02121.01 Camels
#'

for(i in 1:length(milkItems)){
    milkingAnimalsDataToModel[measuredItemCPC== animalMilkCorrespondence[,animal_item_cpc][i], measuredItemCPC:=animalMilkCorrespondence[,milk_item_cpc][i]]
}

## ---------------------------------------------------------------------  


milkFormulaTable =
    getProductionFormula(itemCode = milkItems[1])

milkFormulaParameters =
    with(milkFormulaTable,
         productionFormulaParameters(datasetConfig = datasetConfig,
                                     productionCode = output,
                                     areaHarvestedCode = input,
                                     yieldCode = productivity,
                                     unitConversion = unitConversion)
    )


milkingAnimalsDataToModel=denormalise(milkingAnimalsDataToModel,denormaliseKey="measuredElement", fillEmptyRecords=TRUE)

milkingFinalDataToModel=milkingAnimalsDataToModel[!is.na(get(milkFormulaParameters$areaHarvestedValue))]
milkingFinalDataToModel=milkingFinalDataToModel[!is.na(get(livestockFormulaParameters$productionValue))]
milkingFinalDataToModel=milkingAnimalsDataToModel[get(milkFormulaParameters$areaHarvestedValue)!=0]


milkingAnimalModel = 
    lmer(log(get(milkFormulaParameters$areaHarvestedValue)) ~ 0+
             timePointYears + 
             (0+log(get(livestockFormulaParameters$productionValue))|measuredItemCPC/measuredItemCPC:geographicAreaM49),
         data = milkingFinalDataToModel)


## I had to filter out those lines where livestock numbers are not available and consequently milking animals cannot be computed
## This issue arose when I had to go back to lme4 1.1-7 instead of 1.1-12

milkingAnimalsDataToModel=milkingAnimalsDataToModel[!is.na(get(livestockFormulaParameters$productionValue))]

milkingAnimalsDataToModel[, predicted:=exp(predict(milkingAnimalModel,
                                                   newdata = milkingAnimalsDataToModel,
                                                   allow.new.levels = TRUE))]

toBeImputed=milkingAnimalsDataToModel[,get(milkFormulaParameters$areaHarvestedObservationFlag) ]==processingParameters$missingValueObservationFlag & 
    milkingAnimalsDataToModel[,get(milkFormulaParameters$areaHarvestedMethodFlag) ]==processingParameters$missingValueMethodFlag &
    !is.na(milkingAnimalsDataToModel[,predicted])

##milkingAnimalsDataToModel[toBeImputed, get(milkFormulaParameters$areaHarvestedValue):= predicted]
##milkingAnimalsDataToModel[toBeImputed, get(milkFormulaParameters$areaHarvestedObservationFlag) :="I"]
##milkingAnimalsDataToModel[toBeImputed, get(milkFormulaParameters$areaHarvestedMethodFlag) :="e"]


milkingAnimalsDataToModel[toBeImputed, Value_measuredElement_5318:= predicted]
milkingAnimalsDataToModel[toBeImputed, flagObservationStatus_measuredElement_5318 :=processingParameters$imputationObservationFlag]
milkingAnimalsDataToModel[toBeImputed, flagMethod_measuredElement_5318 :=processingParameters$imputationMethodFlag]

milkingAnimalsDataToModel[,predicted:=NULL]

milkingAnimals=normalise(milkingAnimalsDataToModel)

milkingAnimals=milkingAnimals[measuredElement==milkFormulaParameters$areaHarvestedCode ]


## ---------------------------------------------------------------------  
##Pull milk production

completeImputationKey@dimensions$measuredElement@keys= c(milkFormulaParameters$productionCode)
completeImputationKey@dimensions$measuredItemCPC@keys=milkItems
milkProductionData=GetData(completeImputationKey) 

milkProductionData= removeNonProtectedFlag(milkProductionData)


milkProductionDataToModel=rbind(milkingAnimals,
                                milkProductionData)


milkProductionDataToModel=preProcessing(milkProductionDataToModel)
milkProductionDataToModel=denormalise(milkProductionDataToModel,denormaliseKey="measuredElement", fillEmptyRecords = TRUE)




milkProdFinalDataToModel=milkProductionDataToModel[!is.na(get(milkFormulaParameters$areaHarvestedValue))]
milkProdFinalDataToModel=milkProdFinalDataToModel[!is.na(get(milkFormulaParameters$productionValue))]
milkProdFinalDataToModel=milkProdFinalDataToModel[get(milkFormulaParameters$productionValue)!=0]


milkLmeModel = 
    lmer(log(get(milkFormulaParameters$productionValue)) ~ 0+
             timePointYears + 
             (0+log(get(milkFormulaParameters$areaHarvestedValue))|measuredItemCPC/measuredItemCPC:geographicAreaM49),
         data = milkProdFinalDataToModel)


milkProductionDataToModel=milkProductionDataToModel[!is.na(get(milkFormulaParameters$areaHarvestedValue))]


milkProductionDataToModel[, predicted:=exp(predict(milkLmeModel,
                                                   newdata = milkProductionDataToModel,
                                                   allow.new.levels = TRUE))]




milkProductionToBeImputed=     milkProductionDataToModel[,get(milkFormulaParameters$productionObservationFlag)]==processingParameters$missingValueObservationFlag & 
    milkProductionDataToModel[,get(milkFormulaParameters$productionMethodFlag)]==processingParameters$missingValueMethodFlag &
    !is.na(milkProductionDataToModel[,predicted])



milkProductionDataToModel[milkProductionToBeImputed, Value_measuredElement_5510 := predicted]
milkProductionDataToModel[milkProductionToBeImputed, flagObservationStatus_measuredElement_5510 := processingParameters$imputationObservationFlag]
milkProductionDataToModel[milkProductionToBeImputed, flagMethod_measuredElement_5510 := processingParameters$imputationMethodFlag]


milkProductionFinalImputedData=normalise(milkProductionDataToModel)



## ---------------------------------------------------------------------  
## Finalize the triplet computing the milk-yield 
completeImputationKey@dimensions$measuredElement@keys= c(milkFormulaParameters$yieldCode)

completeImputationKey@dimensions$measuredItemCPC@keys= milkItems
milkYieldProductionData=GetData(completeImputationKey) 

milkYieldProductionData= removeNonProtectedFlag(milkYieldProductionData)
milkYieldProductionData= remove0M(milkYieldProductionData, valueVars = "Value",flagVars = "flagObservationStatus")

completeTriplet=rbind(milkProductionFinalImputedData,
                      milkYieldProductionData)


completeTriplet=denormalise(completeTriplet, denormaliseKey = "measuredElement")


milkYieldToBeImputed= is.na(completeTriplet[,get(milkFormulaParameters$yieldValue)])




completeTriplet[milkYieldToBeImputed, Value_measuredElement_5417:=(get(milkFormulaParameters$productionValue)/get(milkFormulaParameters$areaHarvestedValue))*1000]

milkYieldImputedFlags=milkYieldToBeImputed & !is.na(completeTriplet[,get(milkFormulaParameters$yieldValue)] )

completeTriplet[milkYieldImputedFlags, flagObservationStatus_measuredElement_5417:=aggregateObservationFlag(get(milkFormulaParameters$productionObservationFlag),
                                                                                                            get(milkFormulaParameters$areaHarvestedObservationFlag))]
completeTriplet[milkYieldImputedFlags, flagMethod_measuredElement_5417:=processingParameters$balanceMethodFlag]


##ensureProductionOutputs(completeTriplet,
##                        processingParameters = processingParameters,
##                        formulaParameters =  milkFormulaParameters,
##                        normalised = FALSE,
##                        testImputed = TRUE,
##                        testCalculated = FALSE,
##                        returnData = FALSE
##)
## ---------------------------------------------------------------------  
## Push data back into the SWS

##Add some checks from the ensure package!

## ensureProtectedData
## ensureProductionOutput

completeTriplet=    normalise(completeTriplet)
completeTriplet=    completeTriplet[flagMethod == "i" |(flagObservationStatus == "I" & flagMethod == "e"),]
completeTriplet=    removeInvalidDates(data = completeTriplet, context = sessionKey) 
completeTriplet=    postProcessing (completeTriplet)

    SaveData(domain = sessionKey@domain,
             dataset = sessionKey@dataset,
             data = completeTriplet)


message("Module finished successfully")
