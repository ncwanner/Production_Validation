##' This test is designed to explore whether all the data in the
##' production domain met the data requirement.
##'
##' All values within the selected sets are imputed
##
##' Definition of the set:
##'
##'     1. Required list of country, item, element and years provided
##'        by the user.
##'
##'        NOTE (Michael): This list is currently obtained from the
##'                        getMainKey() function, this should however
##'                        be changed to read from the requirement
##'                        database.
##'
##'     2. The record need to exist in the system.
##'



##' load the libraries
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

##' set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
savePath = paste0(R_SWS_SHARE_PATH, "/kao/production/imputation_fit/")

##' This return FALSE if on the Statistical Working System
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

    savePath = SETTINGS[["save_imputation_path"]]

}

##' Get user specified imputation selection
imputationSelection = swsContext.computationParams$imputation_selection
sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)

##' Get complete imputation range

completeImputationKey = getCompleteImputationKey("production")

completeImputationData =
    completeImputationKey %>%
    GetData %>%
    filter(!is.na(flagObservationStatus)) %>%
    preProcessing(data = .) %>%
    denormalise(normalisedData = .,
                denormaliseKey = "measuredElement",
                fillEmptyRecords = TRUE)

saveRDS(completeImputationData, file = "completeImputationData.rds")

## Flag distribution

## Missing value percentage

## Total number of time series


