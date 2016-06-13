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

sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)

##' Get complete imputation range

completeImputationKey = getCompleteImputationKey("production")

## NOTE (Michael): Probably better to get the data normalised
completeImputationData =
    completeImputationKey %>%
    GetData %>%
    preProcessing(data = .)

saveRDS(completeImputationData, file = "completeImputationData.rds")

completeProductionData = readRDS("completeImputationData.rds")

combineFlag = function(flagObservationStatus, flagMethod){
    paste0("(", flagObservationStatus, ", ", flagMethod, ")")
}

## Flag distribution
completeImputationData[, `:=`("flagCombination",
                                        combineFlag(flagObservationStatus,
                                                    flagMethod))]

flagPctTable =
    as.data.frame(round(table(completeImputationData[["flagCombination"]])/
                        nrow(completeImputationData) * 100, 2))
colnames(flagPctTable) = c("flag combination", "percentage")

## Missing value percentage
sum(completeImputationData[["flagObservationStatus"]] == "M")/nrow(completeImputationData) * 100


countTimeSeries = function(data, key){
    timeSeriesMissing = data[, sum(is.na(Value)) == .N, by = key]
    totalTimeSeries = nrow(timeSeriesMissing)
    nonMissingTimeSeries = sum(!timeSeriesMissing$V1)
    missingTimeSeries = sum(timeSeriesMissing$V1)
    list(totalTimeSeries = totalTimeSeries,
         nonMissingTimeSeries = nonMissingTimeSeries,
         missingTimeSeries =missingTimeSeries)
}

## Total number of time series
timeSeriesCount =
    countTimeSeries(completeImputationData,
                key = c("geographicAreaM49", "measuredElement", "measuredItemCPC"))

## Plot the triplet for all the items
