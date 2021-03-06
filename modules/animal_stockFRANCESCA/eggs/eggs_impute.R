#' EGGS IMPUTATION Sub-module
#' 
#' eggsItems=c("0231","0232") which correspond to their relative animalItem 
#' c("02151","02194"). The correspondence between the milk and animal item is
#' contained in the table animalMilkCorrespondence.RData file stored in the package. The module works with
#' items (both milk and animal) which share the same Elements.
#' Livestock= 5112 (heads)
#' Laying (heads) [input]
#' Eggs Prod= 5510 (tons)  [output]
#' Yiels= 5424    (kg/heads)  [productivity]
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
    SETTINGS = ReadSettings("modules/animal_stockFRANCESCA/sws.yml")
    
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



imputationTimeWindow = swsContext.computationParams$imputation_timeWindow
if(!imputationTimeWindow %in% c("all", "lastThree"))
    stop("Incorrect imputation selection specified")


##' Build processing parameters
processingParameters =
    productionProcessingParameters(datasetConfig = datasetConfig)

##' Obtain the complete imputation key

completeImputationKey = getCompleteImputationKey("production")
lastYear=max(as.numeric(completeImputationKey@dimensions$timePointYears@keys))

##' Here I need to take the data table directly from the SWS


animalEggsCorrespondence=ReadDatatable("animal_eggs_correspondence")

livestockItems = animalEggsCorrespondence[, animal_item_cpc]


livestockFormulaTable =
    getProductionFormula(itemCode = livestockItems[1])



livestockFormulaParameters =
    with(
        livestockFormulaTable,
        productionFormulaParameters(
            datasetConfig = datasetConfig,
            productionCode = output,
            areaHarvestedCode = input,
            yieldCode = productivity,
            unitConversion = unitConversion
        )
    )


##Pull livestock numbers

completeImputationKey@dimensions$measuredItemCPC@keys = livestockItems
completeImputationKey@dimensions$measuredElement@keys = livestockFormulaParameters$productionCode

livestockData = GetData(completeImputationKey)



## ---------------------------------------------------------------------
##Pull eggs animals

itemMap = GetCodeList(domain = "agriculture", dataset = "aproduction", "measuredItemCPC")
eggsItems = itemMap[type == "EGGW", code]


eggsFormulaTable=
    getProductionFormula(itemCode = eggsItems[1])

eggsFormulaParameters =
    with(
       eggsFormulaTable,
        productionFormulaParameters(
            datasetConfig = datasetConfig,
            productionCode = output,
            areaHarvestedCode = input,
            yieldCode = productivity,
            unitConversion = unitConversion
        )
    )

eggsAnimalKey = completeImputationKey
eggsAnimalKey@dimensions$measuredItemCPC@keys = eggsItems
eggsAnimalKey@dimensions$measuredElement@keys = eggsFormulaParameters$areaHarvestedCode

eggsAnimalData = GetData(eggsAnimalKey)
eggsAnimalData=preProcessing(data = eggsAnimalData) 
eggsAnimalData=expandYear(eggsAnimalData, newYears=lastYear)
## remove non protected flag combinations for eggs animals which is the variable that has to be imputed
## if the user has seleceted "last three years" this means that we have to keep all the data upto last three years


if(imputationTimeWindow=="all"){eggsAnimalData=removeNonProtectedFlag(eggsAnimalData)}else{
    eggsAnimalData=removeNonProtectedFlag(eggsAnimalData, keepDataUntil = (lastYear-2))}

#eggsAnimalData = removeNonProtectedFlag(eggsAnimalData)

##Impute eggs animals

eggsAnimalsDataToModel = rbind(livestockData, eggsAnimalData)

eggsAnimalsDataToModel = preProcessing(eggsAnimalsDataToModel)


#'
#'  In this data I have 2 different Typologies of commodities:
#'  1) animals
#'  2) eggs
#'  I have to convert the animal commodities into their correspondet egg commodities
#'  02211         Chicken           0251    chicken
#'  02211         Other birds       0294    Other birds
#'

for (i in 1:length(eggsItems)) {
    eggsAnimalsDataToModel[measuredItemCPC == animalEggsCorrespondence[, animal_item_cpc][i], measuredItemCPC :=
                               animalEggsCorrespondence[, eggs_item_cpc][i]]
}

## ---------------------------------------------------------------------
#       geoTree=GetCodeTree("agriculture", "aproduction", "geographicAreaM49")
#       countryGroups=c("931","934","915","950", "951", "952")
#       geoHierachy=list()
#       
#       for(i in seq_along(countryGroups)){
#           geographicAreaM49=unlist(strsplit(geoTree[parent==countryGroups[i], children], split=", "))
#           geoHierachy[[i]]=data.table(geographicAreaM49)
#           geoHierachy[[i]][, geoParent:=countryGroups[i]]
#       }
#       
#       
#       geoHierachy=unique(rbindlist(geoHierachy))
#       geoHierachy=unique(geoHierachy)
#       
#       
## ---------------------------------------------------------------------

eggsAnimalsDataToModel = denormalise(eggsAnimalsDataToModel,
                                     denormaliseKey = "measuredElement",
                                     fillEmptyRecords = TRUE)
# This is the dataset to build the module
eggsAnimalFinalDataToModel = eggsAnimalsDataToModel[!is.na(get(eggsFormulaParameters$areaHarvestedValue))]
eggsAnimalFinalDataToModel = eggsAnimalFinalDataToModel[!is.na(get(livestockFormulaParameters$productionValue))]
eggsAnimalFinalDataToModel = eggsAnimalsDataToModel[get(eggsFormulaParameters$areaHarvestedValue) !=0]


eggsAnimalModel =lmer(  log(get(eggsFormulaParameters$areaHarvestedValue)) ~ 0 +
            timePointYears +(0 + log(get(livestockFormulaParameters$productionValue)) |
                                 measuredItemCPC/ measuredItemCPC:geographicAreaM49),
        data = eggsAnimalFinalDataToModel)

## Here I am trying to use the info of geographical groups: since "other birds" may be pigeons - geese or duck and 
## it could depend on the geogrpahic area.
#   eggsAnimalsDataToModelTEST=merge(eggsAnimalsDataToModel,geoHierachy, by="geographicAreaM49")
#
#
#   eggsAnimalModel =lmer(  log(get(eggsFormulaParameters$areaHarvestedValue)) ~ 0 +
#                            timePointYears +(0 + log(get(livestockFormulaParameters$productionValue)) | measuredItemCPC:geographicAreaM49:geoParent),
#                        data = eggsAnimalsDataToModelTEST)
#

# I am excluding those items where no livestock number exists.
eggsAnimalsDataToModel=eggsAnimalsDataToModel[!is.na(get(livestockFormulaParameters$productionValue))]    
## ---------------------------------------------------------------------
#eggsAnimalsDataToModel=merge(eggsAnimalsDataToModel, geoHierachy, by="geographicAreaM49")
## ---------------------------------------------------------------------
eggsAnimalsDataToModel[, predicted := exp(predict( eggsAnimalModel,
                                                    newdata = eggsAnimalsDataToModel,
                                                    allow.new.levels = TRUE))]


    toBeImputed = eggsAnimalsDataToModel[, get(eggsFormulaParameters$areaHarvestedObservationFlag)] ==
        processingParameters$missingValueObservationFlag &
        eggsAnimalsDataToModel[, get(eggsFormulaParameters$areaHarvestedMethodFlag)] ==
        processingParameters$missingValueMethodFlag &
        !is.na(eggsAnimalsDataToModel[, predicted])




eggsAnimalsDataToModel[toBeImputed, Value_measuredElement_5313 := predicted]
eggsAnimalsDataToModel[toBeImputed, flagObservationStatus_measuredElement_5313 :=
                           processingParameters$imputationObservationFlag]
eggsAnimalsDataToModel[toBeImputed, flagMethod_measuredElement_5313 := processingParameters$imputationMethodFlag]

eggsAnimalsDataToModel[, predicted := NULL]

eggsAnimals = normalise(eggsAnimalsDataToModel)

eggsAnimals = eggsAnimals[measuredElement == eggsFormulaParameters$areaHarvestedCode]


## ---------------------------------------------------------------------
##Pull eggs production

completeImputationKey@dimensions$measuredElement@keys = c(eggsFormulaParameters$productionCode)
completeImputationKey@dimensions$measuredItemCPC@keys = eggsItems

eggsProductionData = GetData(completeImputationKey)
eggsProductionData=preProcessing(data = eggsProductionData) 
eggsProductionData=expandYear(eggsProductionData, newYears=lastYear)


## Isolate those series for which egg items already exist:
## The linear mixed model creates milk production wherever livestock number exist.
## Here we want to be sure to not create a series from scratch
## Even if: in teory if we have livestock numbers whe should have milk production.
## ---------------------------------------------------------------------
existingSeries = unique(eggsProductionData[,.(geographicAreaM49 , measuredItemCPC )])
## ---------------------------------------------------------------------


if(imputationTimeWindow=="all"){eggsProductionData=removeNonProtectedFlag(eggsProductionData)}else{
    eggsProductionData=removeNonProtectedFlag(eggsProductionData, keepDataUntil = (lastYear-2))}

#eggsProductionData = removeNonProtectedFlag(eggsProductionData)


eggsProductionDataToModel = rbind(eggsAnimals,
                                  eggsProductionData)


eggsProductionDataToModel = preProcessing(eggsProductionDataToModel)
eggsProductionDataToModel = denormalise(eggsProductionDataToModel,
                                        denormaliseKey = "measuredElement",
                                        fillEmptyRecords = TRUE)




eggsProdFinalDataToModel = eggsProductionDataToModel[!is.na(get(eggsFormulaParameters$areaHarvestedValue))]
eggsProdFinalDataToModel = eggsProdFinalDataToModel[!is.na(get(eggsFormulaParameters$productionValue))]
eggsProdFinalDataToModel = eggsProdFinalDataToModel[get(eggsFormulaParameters$productionValue) != 0]
                                                       


eggsLmeModel =
    lmer( log(get(eggsFormulaParameters$productionValue)) ~ 0 +
            timePointYears +    (  0 + log(get(
                    eggsFormulaParameters$areaHarvestedValue  )) 
              | measuredItemCPC / measuredItemCPC:geographicAreaM49),
        data = eggsProdFinalDataToModel
    )


eggsProductionDataToModel=eggsProductionDataToModel[!is.na(get(eggsFormulaParameters$areaHarvestedValue))]


eggsProductionDataToModel[, predicted := exp(predict(
    eggsLmeModel,
    newdata = eggsProductionDataToModel,
    allow.new.levels = TRUE))]



eggsProductionToBeImputed =     eggsProductionDataToModel[, get(eggsFormulaParameters$productionObservationFlag)] ==
    processingParameters$missingValueObservationFlag &
    eggsProductionDataToModel[, get(eggsFormulaParameters$productionMethodFlag)] ==
    processingParameters$missingValueMethodFlag &
    !is.na(eggsProductionDataToModel[, predicted])



eggsProductionDataToModel[eggsProductionToBeImputed, Value_measuredElement_5510 := predicted]
eggsProductionDataToModel[eggsProductionToBeImputed, flagObservationStatus_measuredElement_5510 := processingParameters$imputationObservationFlag]
eggsProductionDataToModel[eggsProductionToBeImputed, flagMethod_measuredElement_5510 := processingParameters$imputationMethodFlag]


eggsProductionFinalImputedData = normalise(eggsProductionDataToModel)



## ---------------------------------------------------------------------
## Finalize the triplet computing the eggs-yield
completeImputationKey@dimensions$measuredElement@keys = c(eggsFormulaParameters$yieldCode)

completeImputationKey@dimensions$measuredItemCPC@keys = eggsItems
eggsYieldProductionData = GetData(completeImputationKey)
if(imputationTimeWindow=="all"){eggsYieldProductionData=removeNonProtectedFlag(eggsYieldProductionData)}else{
    eggsYieldProductionData=removeNonProtectedFlag(eggsYieldProductionData, keepDataUntil = (lastYear-2))}

#eggsYieldProductionData = removeNonProtectedFlag(eggsYieldProductionData)


completeTriplet = rbind(eggsProductionFinalImputedData,
                        eggsYieldProductionData)


completeTriplet = denormalise(completeTriplet, denormaliseKey = "measuredElement")


completeTriplet=fillRecord(completeTriplet)



##compute yield where necessary:
eggsYieldToBeImputed = is.na(completeTriplet[, get(eggsFormulaParameters$yieldValue)])

completeTriplet[eggsYieldToBeImputed, Value_measuredElement_5424 := (
    get(eggsFormulaParameters$productionValue) / get(eggsFormulaParameters$areaHarvestedValue)
) * 1000]

eggsYieldImputedFlags = as.logical( eggsYieldToBeImputed *  !is.na(completeTriplet[, get(eggsFormulaParameters$yieldValue)]))

completeTriplet[eggsYieldImputedFlags, flagObservationStatus_measuredElement_5424 :=
                    aggregateObservationFlag(
                        get(eggsFormulaParameters$productionObservationFlag),
                        get(eggsFormulaParameters$areaHarvestedObservationFlag)
                    )]
completeTriplet[eggsYieldImputedFlags, flagMethod_measuredElement_5424 :=
                    processingParameters$balanceMethodFlag]



#Manage (M,-)

## if animal milk numbers are (M,-)

MdashEggsAnimal=completeTriplet[,get(eggsFormulaParameters$areaHarvestedObservationFlag)]=="M"&
    completeTriplet[,get(eggsFormulaParameters$areaHarvestedMethodFlag)]=="-"


completeTriplet[MdashEggsAnimal,":="(c(eggsFormulaParameters$yieldValue,eggsFormulaParameters$yieldObservationFlag,eggsFormulaParameters$yieldMethodFlag),
                                        list(NA_real_,"M", "-"))]


## if production of milk numbers are (M,-)
MdashEggsProd=completeTriplet[,get(eggsFormulaParameters$productionObservationFlag)]=="M"&
    completeTriplet[,get(eggsFormulaParameters$productionMethodFlag)]=="-"

completeTriplet[MdashEggsProd,":="(c(eggsFormulaParameters$yieldValue,eggsFormulaParameters$yieldObservationFlag,eggsFormulaParameters$yieldMethodFlag),
                                   list(NA_real_,"M", "-"))]

         
## ---------------------------------------------------------------------
## Push data back into the SWS

##Add some checks from the ensure package!

## ensureProtectedData
## ensureProductionOutput

completeTriplet=    normalise(completeTriplet)
completeTriplet = removeInvalidDates(data = completeTriplet, context = sessionKey) 
completeTriplet = postProcessing (completeTriplet)

#ensureProtectedData(completeTriplet,domain = "agriculture", "aproduction",getInvalidData = TRUE)

ensureProductionOutputs(
    completeTriplet,
    processingParameters = processingParameters,
    formulaParameters =  eggsFormulaParameters,
    normalised = TRUE,
    testImputed = FALSE,
    testCalculated = TRUE,
    returnData = FALSE
)



completeTriplet=    completeTriplet[flagMethod == "i" |(flagObservationStatus == "I" & flagMethod == "e")|
                                        (flagObservationStatus == "M" & flagMethod == "-"),]

completeTriplet=completeTriplet[existingSeries, , on=c("geographicAreaM49", "measuredItemCPC")]
completeTriplet=completeTriplet[!is.na(measuredElement)]



if(imputationTimeWindow=="lastThree"){
    completeTriplet=completeTriplet[timePointYears %in% c(lastYear, lastYear-1, lastYear-2), ]
    SaveData(domain = sessionKey@domain, 
             dataset = sessionKey@dataset,
             data =  completeTriplet) 
    
    }else{
SaveData(domain = sessionKey@domain, 
         dataset = sessionKey@dataset,
         data= completeTriplet)
}

if(!CheckDebug()){
    ## Initiate email
    from = "sws@fao.org"
    to = swsContext.userEmail
    subject = "Eggs production module"
    body = paste0("Eggs production module successfully ran. You can browse results in the session: ", sessionKey@sessionId )
    sendmail(from = from, to = to, subject = subject, msg = body)
}

message("Module finished successfully")
