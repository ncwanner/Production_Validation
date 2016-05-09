suppressMessages({
    library(faosws)
    library(faoswsUtil)
    library(data.table)
    library(magrittr)
    library(dplyr)
})

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
server = "QA"

if(CheckDebug()){
    cat("Not on server, so setting up environment...\n")
    if(server == "Prod"){
        ## Define directories
        if(Sys.info()[7] == "josh"){
            apiDirectory = "~/Documents/Github/faoswsProduction/R/"
            R_SWS_SHARE_PATH = "/media/hqlprsws2_prod/"
            ## R_SWS_SHARE_PATH = "/media/hqlprsws2_prod"
            SetClientFiles(dir = "~/R certificate files/Production/")
        } else if(Sys.info()[7] == "rockc_000"){
            apiDirectory = "~/Github/faoswsProduction/R/"
            stop("Can't connect to share drives!")
        } else if(Sys.info()[7] == "mk"){
            apiDirectory = "R/"
            R_SWS_SHARE_PATH = "/media/sws_prod_shared_drive/"
            SetClientFiles(dir = "~/.R/prod")
        } else if(Sys.info()[7] == "kao"){
            apiDirectory = "R/"
            R_SWS_SHARE_PATH = "/media/sws_prod_shared_drive/"
            SetClientFiles(dir = "~/.R/prod")
        }
        ## Get SWS Parameters
        GetTestEnvironment(
            baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
            token = "e518d5c0-7316-4f21-9f6f-4d2aa666c0c2"
            ## baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
            ## token = "b55f4ae3-5a0c-4514-b89e-d040112bf25e"
        )
    } else if(server == "QA"){
        if(Sys.info()["user"] == "mk"){
            SetClientFiles("~/.R/qa/")
            R_SWS_SHARE_PATH = "/media/sws_qa_shared_drive"
            apiDirectory = "R/"
        }
        GetTestEnvironment(
            baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
            ## token = "f8646896-2ed2-4e88-9cd2-9db6d735991f"
            ## New Token for all the meats
            token = "732603a1-f4ea-40d8-8858-b31e41b0092c"
        )
    } else {
        stop("Please specify a valid server name")
    }
    ## Source local scripts for this local test
    for(file in dir(apiDirectory, full.names = T))
        source(file)
}


modelLoadingPath = paste0(R_SWS_SHARE_PATH, "/kao/production/imputation_fit/")
selectedKey = swsContext.datasets[[1]]


## NOTE (Michael): The imputation and all modules should now have a base year of
##                 1999, this is the result of a discussion with Pietro.
defaultYear = 1999
imputationYears =
    GetCodeList(domain = "agriculture",
                dataset = "aproduction",
                dimension = "timePointYears") %>%
    filter(description != "wildcard" & as.numeric(code) >= defaultYear) %>%
    select(code) %>%
    unlist(use.names = FALSE)


## Get the complete set of keys for imputation
completeImputationKey = getMainKey(imputationYears)

## Subset the item of the complete key to the selected items. We use the
## complete key to subset as the selection may not include the full list of
## year/country/elements
selectedImputationItems = selectImputationItem(selectedKey, completeImputationKey)

## Test whether the selected keys are in the complete key, if not in
## the completeImputationKey, then it will be returned as a message
## that the item should probably be added in.
nonImputationItemCodes = nonImputationItems(selectedKey, completeImputationKey)

## Start the imputation, looping through each item since the model are
## fitted for each item separately.
for(i in seq(selectedImputationItems)){
    ## Select the item and the associate formula/elements
    currentItem = selectedImputationItems[i]
    currentFormula = getYieldFormula(currentItem)
    currentElements =
        currentFormula %>%
        select(input, productivity, output) %>%
        unlist(x = ., use.names = FALSE)

    ## Give warning if the imputed data set does not exist
    if(!imputationExist(modelLoadingPath = modelLoadingPath,
                        item = currentItem))
        stop("Imputation does not exist for item '", currentItem,
             "'. Please run imputation model first.")

    ## TODO (Michael): This creates the name of the imputation
    ##                 object. Probably need a function to standardise
    ##                 and sync this with the create_imputed_dataset
    ##                 module.
    modelName = createImputationObjectName(item = currentItem)

    ## Modify the selected key
    subKey = selectedKey
    subKey@dimensions$measuredItemCPC@keys = currentItem
    subKey@dimensions$measuredElement@keys = currentElements

    print(paste0("Filling missing values for item: ",
                 currentItem, " (",  i, " out of ",
                 length(selectedImputationItems),")"))

    ## Load the fitted values
    imputedValues =
        readRDS(file = paste0(modelLoadingPath, modelName)) %>%
        preProcessing(data = .) %>%
        filter(flagObservationStatus == "I" & flagMethod %in% c("i", "e")) %>%
        setkeyv(x = ., col = c("geographicAreaM49", "measuredItemCPC",
                               "timePointYears", "measuredElement"))

    ## Load the selected data from the data base
    currentValues =
        GetData(subKey) %>%
        preProcessing(data = .) %>%
        setkeyv(x = , col = c("geographicAreaM49", "measuredItemCPC",
                              "timePointYears", "measuredElement"))

    ## NOTE (Michael): Need to think about what should be
    ##                 updated. Should we over-write all the data with
    ##                 specific set of flags, or only values that are
    ##                 missing?

    saveResult =
        {
            ## NOTE (Michael): Only impute production if the item is
            ##                 not a primary commodity.
            if(isPrimary(currentItem)){
                return(currentValues)
            } else {
                return(currentValues[measuredElement %in%
                                     currentFormula[, output], ])
            }
        } %>%
        ## Inner join with the imputed data
        ##
        ## TODO (Michael): Need to add a check here if the
        ##                 imputedValues dataset is smaller than the
        ##                 selected values. This is an indication that
        ##                 the imputation does not perform on all
        ##                 data.
        .[imputedValues, ] %>%
        ## Subset only flags that can be over-written
        ##
        ## TODO (Michael): Discuss with team B and C whether "T" should be
        ##                 imputed and over-written.
        filter(flagObservationStatus %in% c("I", "E", "M")) %>%
        ## Assign the imputed value to the current dataset
        mutate(Value = i.Value,
               flagObservationStatus = i.flagObservationStatus,
               flagMethod = i.flagMethod) %>%
        ## Remove imputation column
        select(.data = ., select = -starts_with("i.")) %>%
        postProcessing(data = .) %>%
        checkProtectedData(dataToBeSaved = .) %>%
        ## NOTE (Michael): flagMethod can be 'i' or 'e' since yield
        ##                 can be computed as an identity during the
        ##                 imputation stage.
        checkOutputFlags(data = .,
                         flagObservationStatusExpected = "I",
                         flagMethodExpected = c("i", "e")) %>%
        ## NOTE (Michael): This test might fail because the data may have been
        ##                 updated since the production imputation module was
        ##                 performed.
        checkProductionBalanced(data = .,
                                areaVar = currentFormula[, input],
                                yieldVar = currentFormula[, productivity],
                                prodVar = currentFormula[, output],
                                conversion = currentFormula[, unitConversion]) %>%
        ## Save data back
        SaveData(domain = "agriculture", dataset = "aproduction", data = .)
}

## Return message
yearOutOfImputationRange =
    setdiff(selectedKey@dimensions$timePointYears@keys, imputationYears)

yearWarningMessage =
    ifelse(length(yearOutOfImputationRange) > 0,
           paste0("\n\nThe following selected years were not imputed as they ",
                  "are out of range (1999 - current year): \n",
                  paste0(yearOutOfImputationRange, collapse = ", "), "\n"),
           "")


commodityWarningMessage =
    ifelse(length(nonImputationItemCodes) > 0,
           paste0("\n\n The following selected commodities were not ",
                  "imputed as they are not included in the current list:\n",
                  paste0(nonImputationItemCodes, collapse = ", "), "\n"),
           "")

finalMessage =
    paste0("Imputation module executed successfully\n",
           yearWarningMessage,
           commodityWarningMessage)

message(finalMessage)
