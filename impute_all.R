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

## NOTE (Michael): The selected item should be based on the main key.
completeImputationKey = getMainKey(1992:2014)

## Test whether the selected keys are in the complete key, if not in
## the completeImputationKey, then it will be returned as a message
## that the item should probably be added in.
nonImputationItems = function(selectedKey, imputationKey){
    selectedItems = selectedKey@dimensions$measuredItemCPC@keys
    imputationItems = imputationKey@dimensions$measuredItemCPC@keys
    nonImputationItems = setdiff(selectedItems, imputationItems)
    nonImputationItems
}


imputationExist = function(modelLoadingPath, item, modelPrefix = "imputation_"){
    modelName = paste0("imputation_", item, ".rds")
    file.exists(paste0(modelLoadingPath, modelName))
}

selectImputationItem = function(selectedKey, imputationKey){
    selectedItems = selectedKey@dimensions$measuredItemCPC@keys
    imputationItems = imputationKey@dimensions$measuredItemCPC@keys
    selectedImputationItems = intersect(selectedItems, imputationItems)
    selectedImputationItems
}

selectedImputationItems = selectImputationItem(selectedKey, completeImputationKey)

nonImputationItemCodes = nonImputationItems(selectedKey, completeImputationKey)


## selectedImputationItems = "0111"

## Start the imputation, looping through each item since the model are
## fitted for each item separately.
for(i in seq(selectedImputationItems)){
    currentItem = selectedImputationItems[i]
    
    if(!imputationExist(modelLoadingPath = modelLoadingPath,
                        item = currentItem))
        stop("Imputation does not exist for item '", currentItem,
             "'. Please run imputation model first.")

    modelName = paste0("imputation_", currentItem, ".rds")
    subKey = selectedKey
    subKey@dimensions$measuredItemCPC@keys = currentItem
    print(paste0("Imputation for item: ", currentItem, " (",  i, " out of ",
                 length(selectedImputationItems),")"))

    ## Load the fitted values
    imputedValues =
        readRDS(file = paste0(modelLoadingPath, modelName)) %>%
        preProcessing(data = .) %>%
        setkeyv(x = ., col = c("geographicAreaM49", "measuredItemCPC",
                               "timePointYears", "measuredElement"))
    
    
    ## Load the current data
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
        ## TODO (Michael): Need to check this join
        currentValues[imputedValues, ] %>%
        ## Subset only flags that can be over-written
        filter(flagObservationStatus %in% c("I", "E", "M")) %>%
        ## Assign the imputed value
        mutate(Value = i.Value,
               flagObservationStatus = i.flagObservationStatus,
               flagMethod = i.flagMethod) %>%
        ## Remove imputation column
        select(.data = ., select = -starts_with("i."))##  %>%
        ## ## Save data back
        ## SaveData(domain = "agriculture", dataset = "aproduction", data = .)
    
}



## Return message
paste0("Imputation module executed successfully, the following seelcted commodities were not imputed as they are not in the imputation list provided\n", paste0(nonImputationItemCodes, collapse = "\n"))
