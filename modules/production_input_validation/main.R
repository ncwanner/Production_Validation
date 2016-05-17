## This is a sample main file for a module
suppressMessages({
    library(faosws)
    library(faoswsUtil)
    library(faoswsFlag)
    library(faoswsImputation)
    library(faoswsProduction)
    library(faoswsProcessing)
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


## NOTE (Michael): The imputation and all modules should now have a base year of
##                 1999, this is the result of a discussion with Pietro.
defaultYear = 1999
imputationYears =
    GetCodeList(domain = sessionKey@domain,
                dataset = sessionKey@dataset,
                dimension = "timePointYears") %>%
    filter(description != "wildcard" & as.numeric(code) >= defaultYear) %>%
    select(code) %>%
    unlist(use.names = FALSE)

##' Obtain the complete imputation Datakey
completeImputationKey = getMainKey(years = imputationYears)

##' Selected the key based on the input parameter
selectedKey =
    switch(validationRange,
           "session" = sessionKey,
           "all" = completeImputationKey)

##' Get the data from the Statistical Working System

selectedData = GetData(selectedKey)

## Build processing parameters
##
## TODO (Michael): Split this into general processing parameters, and
##                 yield formula parameters (or can put into the
##                 imputation parameters)
processingParameters =
    productionProcessingParameters(
        datasetConfig = datasetConfig,
        productionCode = currentFormula$output,
        areaHarvestedCode = currentFormula$input,
        yieldCode = currentFormula$productivity,
        unitConversion = currentFormula$unitConversion)

##' Perform the validation
##
## NOTE (Michael): Need to loop through difference formulas

selectedData %>%
    ensureProductionInputs(data = .,
                           processingParameters = processingParameters)

message("Production Input Validation passed without any error!")
