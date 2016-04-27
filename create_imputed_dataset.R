## load the library
suppressMessages({
    library(faosws)
    library(faoswsUtil)
    library(faoswsFlag)
    library(faoswsImputation)
    library(data.table)
    library(splines)
    library(lme4)
    library(magrittr)
})

## Setting up variables
yearsModeled = 20

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
savePath = paste0(R_SWS_SHARE_PATH, "kao/production/")

if(CheckDebug()){
    cat("Not on server, so setting up environment...\n")
    
    server = "Prod"
    # server = "Prod"
    
    stopifnot(server %in% c("QA", "Prod"))
    ## Define directories
    if(Sys.info()[7] == "josh"){
        apiDirectory = "~/Documents/Github/faoswsProduction/R/"
        R_SWS_SHARE_PATH = ifelse(server == "Prod", "/media/hqlprsws2_prod/",
                                  "/media/hqlprsws1_qa/")
        SetClientFiles(dir = ifelse(server == "Prod",
                                    "~/R certificate files/Production/",
                                    "~/R certificate files/QA/"))
        # runParallel = TRUE
        ## Get SWS Parameters
        SetClientFiles(dir = ifelse(server == "Prod",
                                    "~/R certificate files/Production/",
                                    "~/R certificate files/QA/"))
    } else if(Sys.info()[7] == "rockc_000"){
        apiDirectory = "~/Github/faoswsProduction/R/"
        stop("Can't connect to share drives!")
    } else if(Sys.info()[7] == "mk"){
        apiDirectory = "R/"
        R_SWS_SHARE_PATH = ifelse(server == "Prod",
                                  "/media/sws_prod_shared_drive/",
                                  "/media/sws_qa_shared_drive/")
        SetClientFiles(dir = ifelse(server == "Prod", "~/.R/prod/", "~/.R/qa/"))
        savePath = "imputation_fit/"
    } else if(Sys.info()[7] == "kao"){
        apiDirectory = "R/"
        R_SWS_SHARE_PATH = ifelse(server == "Prod",
                                  "/media/sws_prod_shared_drive/",
                                  "/media/sws_qa_shared_drive/")
        SetClientFiles(dir = ifelse(server == "Prod", "~/.R/prod/", "~/.R/qa/"))
        savePath = "imputation_fit/"
    }


    ## Get SWS Parameters

    if(server == "Prod"){
        GetTestEnvironment(
            baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
            token = "ad797167-20aa-4ff4-b679-461c96e0da79"
        )
    } else {
        GetTestEnvironment(
            baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
            token = "5eed31f5-2318-470e-a884-c30c3db6d3db"
        )
    }

    ## Source local scripts for this local test
    for(file in dir(apiDirectory, full.names = T))
        source(file)

}




## HACK (Michael): This is to catch a specific known error, that is,
##                 corrupted data in the database with
##                 flagObservationStatus taking the value of '-'. This
##                 should be removed later.
allowedErrorMessage = "Some observation flags are not in the flag table!"

allowedError = function(tryErrorObject, allowedError){
    errorMessage = attr(tryErrorObject, "condition")$message
    if(grepl(allowedError, errorMessage)){
        warning("The error ('", errorMessage,
                "')is currently allowed, but should be fixed.")
    } else {
        stop(errorMessage)
    }
}



lastYear = as.numeric(swsContext.computationParams$lastYear)
firstYear = lastYear - yearsModeled + 1 # Rolling time range of yearsModeled years
years = firstYear:lastYear

newKey = getMainKey(years = years)
selectedItemCode = newKey@dimensions[["measuredItemCPC"]]@keys

for(iter in 1:length(selectedItemCode)){
    currentItem = selectedItemCode[iter]
    subKey = newKey
    subKey@dimensions$measuredItemCPC@keys = currentItem
    print(paste0("Imputation for item: ", currentItem, " (",  iter, " out of ",
                 length(selectedItemCode),")"))
    saveFileName = paste0("imputation_", currentItem, ".rds")
    
    imputation = try({

        ## NOTE (Michael): We now impute the full triplet rather than
        ##                 just production for non-primary
        ##                 products. However, in the imputeModel
        ##                 module, only the production will be
        ##                 selected and imputed.
        ##
        ##                 Nevertheless, we hope to change this and
        ##                 impute the full triplet, for all the
        ##                 commodity. We will need to transfer and
        ##                 sync the inputs just like in the meat
        ##                 case. This will allow us to merge the two
        ##                 components.
        ##
        imputed = imputeMeatTriplet(meatKey = subKey)

        imputed %>%
            normalise(.) %>%
            ## Change time point year back to character
            postProcessing(data = .) %>%
            checkTimeSeriesImputed(dataToBeSaved = .,
                                   key = c("geographicAreaM49",
                                           "measuredItemCPC",
                                           "measuredElement"),
                                   valueColumn = "Value") %>%
            checkProtectedData(dataToBeSaved = .) %>%
            saveRDS(object = ., file = paste0(savePath, saveFileName))


    })

    if(!inherits(imputation, "try-error")){
        ## New module test
        message("Imputation Module Executed Successfully!")
    } else {

        allowedError(imputation, allowedError = allowedErrorMessage)
        ## cat(paste0("Item ",  currentItem, " failed : \n",
        ##            imputation[1], "\n\n"),
        ##     file = "imputation.log",
        ##     append = TRUE)

    }
}
