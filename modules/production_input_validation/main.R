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

selectedData = GetData(selectedKey)
selectedItem = unique(selectedData$measuredItemCPC)

##' Perform input validation item by item
for(iter in seq(selectedItem)){
    currentItem = selectedItem[iter]
    message("Performing input validation for item: ", currentItem)
    currentFormula = getYieldFormula(currentItem)

    formulaParameters =
        with(currentFormula,
             productionFormulaParameters(datasetConfig = datasetConfig,
                                         productionCode = output,
                                         areaHarvestedCode = input,
                                         yieldCode = productivity,
                                         unitConversion = unitConversion))

    selectedData %>%
        filter(measuredItemCPC == currentItem) %>%
        preProcessing(data = .) %>%
        ensureProductionInputs(data = .,
                               processingParameters = processingParameters,
                               formulaParameters = formulaParameters)

}
message("Production Input Validation passed without any error!")
