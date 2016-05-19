## This is a sample main file for a module
suppressMessages({
    library(faosws)
    library(faoswsUtil)
    library(faoswsFlag)
    library(faoswsImputation)
    library(faoswsProduction)
    library(faoswsProcessing)
    library(faoswsEnsure)
    library(magrittr)
    library(dplyr)
})


if(CheckDebug()){

    library(faoswsModules)
    SETTINGS = ReadSettings("sws.yml")

    R_SWS_SHARE_PATH = SETTINGS[["share"]]

    ## Define where your certificates are stored
    SetClientFiles(SETTINGS[["certdir"]])

    GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                       token = SETTINGS[["token"]])

}

##' Obtain input computation parameter
validationRange = swsContext.computationParams$validation_selection

##' Get session key and dataset configuration
sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)


##' Obtain the complete imputation Datakey
completeImputationKey = getCompleteImputationKey()

##' Selected the key based on the input parameter
selectedKey =
    switch(validationRange,
           "session" = sessionKey,
           "all" = completeImputationKey)


##' Build processing parameters
processingParameters =
    productionProcessingParameters(datasetConfig = datasetConfig)


##' Extract the data from the Statistical Working System



##' Perform autocorrection, certain incorrectly specified flag can be
##' automatically corrected as per agreement with team B/C.
autoFlagCorrection = function(data,
                              flagObservationStatusVar = "flagObservationtatus",
                              flagMethodVar = "flagMethod"){
    dataCopy = copy(data)

    ## Correction (1): (M, -) --> (M, u)
    correctionFilter =
        dataCopy[[flagObservationStatusVar]] == "M" &
        dataCopy[[flagMethodVar]] == "-"
    dataCopy[correctionFilter,
         `:=`(c(flagObservationStatusVar, flagMethodVar),
              c("M", "u"))]

    ## Correction (2): (E, t) --> (E, -)
    correctionFilter =
        dataCopy[[flagObservationStatusVar]] == "E" &
        dataCopy[[flagMethodVar]] == "t"
    dataCopy[correctionFilter,
         `:=`(c(flagObservationStatusVar, flagMethodVar),
              c("E", "-"))]
    dataCopy
}


autoValueCorrection = function(data,
                               processingParameters,
                               formulaParameters){
    correctedData =
        with(formulaParameters,
             with(processingParameters,
                  data %>%
                  removeZeroConflict(data = .,
                                     value1 = productionValue,
                                     value2 = areaHarvestedValue,
                                     observationFlag1 = productionObservationFlag,
                                     observationFlag2 =
                                         areaHarvestedObservationFlag,
                                     methodFlag1 = productionMethodFlag,
                                     methodFlag2 = areaHarvestedMethodFlag,
                                     missingObservationFlag =
                                         missingValueObservationFlag,
                                     missingMethodFlag = missingValueMethodFlag)
                  )
             )
    correctedData
}


## TODO (Michael): Need to auto correct values. (e.g. conflict area harvested
##                 production value.

selectedItem =
    unique(slot(slot(selectedKey, "dimensions")[[processingParameters$itemVar]],
           "keys"))

##' Perform input validation item by item
for(iter in seq(selectedItem)){
    currentItem = selectedItem[iter]
    message("Performing input validation for item: ", currentItem)
    currentFormula = getYieldFormula(currentItem)
    currentKey = selectedKey
    ## Update the item and element key to for current item
    currentKey@dimensions$measuredItemCPC@keys = currentItem
    currentKey@dimensions$measuredElement@keys =
        with(currentFormula,
             c(input, productivity, output))

    ## Create the formula parameter list
    formulaParameters =
        with(currentFormula,
             productionFormulaParameters(datasetConfig = datasetConfig,
                                         productionCode = output,
                                         areaHarvestedCode = input,
                                         yieldCode = productivity,
                                         unitConversion = unitConversion))

    ## Extract the current data
    currentData =
        currentKey %>%
        GetData(key = .) %>%
        fillRecord(data = .) %>%
        preProcessing(data = .) %>%
        autoFlagCorrection(data = .) %>%
        denormalise(normalisedData = .,
                    denormaliseKey = processingParameters$elementVar) %>%
        autoValueCorrection(data = .,
                            processingParameters = processingParameters,
                            formulaParameters = formulaParameters)


    ## Ensure the inputs are valid
    currentData %>%
        normalise(denormalisedData = .) %>%
        ensureProductionInputs(data = .,
                               processingParameters = processingParameters,
                               formulaParameters = formulaParameters) %>%
        SaveData(domain = sessionKey@domain,
                 dataset = sessionKey@dataset,
                 data = .)
}
message("Production Input Validation passed without any error!")
