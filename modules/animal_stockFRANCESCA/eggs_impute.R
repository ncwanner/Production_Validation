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

if (CheckDebug()) {
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
if (!imputationSelection %in% c("session", "all"))
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

livestockItems = animalEggsCorrespondence[, animalItemCPC]


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

##remove non portected flag combinations for eggs animals which is the variable that has to be imputed

eggsAnimalData = removeNonProtectedFlag(eggsAnimalData)

##Impute eggs animals

eggsAnimalsDataToModel = rbind(livestockData, eggsAnimalData)

eggsAnimalsDataToModel = preProcessing(eggsAnimalsDataToModel)


#'
#'  In this data I have 2 different Typologies of commodities:
#'  1) animals
#'  2) eggs
#'  I have to convert the animal commoties into their correspondet milk commodities
#'  02211         Raw milk of cattle           0251    chicken
#'  02211         Raw milk of cattle           0294    Other birds
#'

for (i in 1:length(eggsItems)) {
    eggsAnimalsDataToModel[measuredItemCPC == animalEggsCorrespondence[, animalItemCPC][i], measuredItemCPC :=
                               animalEggsCorrespondence[, eggsItemCPC][i]]
}

## ---------------------------------------------------------------------


eggsFormulaTable =
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


eggsAnimalsDataToModel = denormalise(eggsAnimalsDataToModel,
                                     denormaliseKey = "measuredElement",
                                     fillEmptyRecords = TRUE)

eggsAnimalFinalDataToModel = eggsAnimalsDataToModel[!is.na(get(eggsFormulaParameters$areaHarvestedValue))]
eggsAnimalFinalDataToModel = eggsAnimalFinalDataToModel[!is.na(get(livestockFormulaParameters$productionValue))]
eggsAnimalFinalDataToModel = eggsAnimalsDataToModel[get(eggsFormulaParameters$areaHarvestedValue) !=0]


eggsAnimalModel =lmer(  log(get(eggsFormulaParameters$areaHarvestedValue)) ~ 0 +
            timePointYears +(0 + log(get(livestockFormulaParameters$productionValue)) |
                                 measuredItemCPC / measuredItemCPC:geographicAreaM49),
        data = eggsAnimalFinalDataToModel)
    

eggsAnimalsDataToModel[, predicted := exp(predict(
    eggsAnimalModel,
    newdata = eggsAnimalsDataToModel,
    allow.new.levels = TRUE
))]

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

eggsProductionData = removeNonProtectedFlag(eggsProductionData)


eggsProductionDataToModel = rbind(eggsAnimals,
                                  eggsProductionData)


eggsProductionDataToModel = preProcessing(eggsProductionDataToModel)
eggsProductionDataToModel = denormalise(eggsProductionDataToModel,
                                        denormaliseKey = "measuredElement",
                                        fillEmptyRecords = TRUE)




eggsProdFinalDataToModel = eggsProductionDataToModel[!is.na(get(eggsFormulaParameters$areaHarvestedValue))]
eggsProdFinalDataToModel = eggsProdFinalDataToModel[!is.na(get(eggsFormulaParameters$productionValue))]
eggsProdFinalDataToModel = eggsProdFinalDataToModel[get(eggsFormulaParameters$productionValue) !=
                                                        0]


eggsLmeModel =
    lmer(
        log(get(eggsFormulaParameters$productionValue)) ~ 0 +
            timePointYears +
            (
                0 + log(get(
                    eggsFormulaParameters$areaHarvestedValue
                )) | measuredItemCPC / measuredItemCPC:geographicAreaM49
            ),
        data = eggsProdFinalDataToModel
    )





eggsProductionDataToModel[, predicted := exp(predict(
    eggsLmeModel,
    newdata = eggsProductionDataToModel,
    allow.new.levels = TRUE
))]




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

eggsYieldProductionData = removeNonProtectedFlag(eggsYieldProductionData)


completeTriplet = rbind(eggsProductionFinalImputedData,
                        eggsYieldProductionData)


completeTriplet = denormalise(completeTriplet, denormaliseKey = "measuredElement")


eggsYieldToBeImputed = is.na(completeTriplet[, get(eggsFormulaParameters$yieldValue)])




completeTriplet[eggsYieldToBeImputed, Value_measuredElement_5424 := (
    get(eggsFormulaParameters$productionValue) / get(eggsFormulaParameters$areaHarvestedValue)
) * 1000]

eggsYieldImputedFlags = eggsYieldToBeImputed *  !is.na(completeTriplet[, eggsFormulaParameters$yieldValue])

completeTriplet[eggsYieldImputedFlags, flagObservationStatus_measuredElement_5424 :=
                    aggregateObservationFlag(
                        get(eggsFormulaParameters$productionObservationFlag),
                        get(eggsFormulaParameters$areaHarvestedObservationFlag)
                    )]
completeTriplet[eggsYieldImputedFlags, flagMethod_measuredElement_5424 :=
                    processingParameters$balanceMethodFlag]


ensureProductionOutputs(
    completeTriplet,
    processingParameters = processingParameters,
    formulaParameters =  eggsFormulaParameters,
    normalised = FALSE,
    testImputed = TRUE,
    testCalculated = FALSE,
    returnData = FALSE
)
## ---------------------------------------------------------------------
## Push data back into the SWS

##Add some checks from the ensure package!

## ensureProtectedData
## ensureProductionOutput

completeTriplet %>%
    normalise(.) %>%
    filter(.,
           flagMethod == "i" |
               (flagObservationStatus == "I" &
                    flagMethod == "e")) %>%
    removeInvalidDates(data = ., context = sessionKey) %>%
    postProcessing %>%
    SaveData(domain = sessionKey@domain,
             dataset = sessionKey@dataset,
             data = .)


message("Module finished successfully")
