## load the library
suppressMessages({
    library(faosws)
    library(faoswsUtil)
    library(faoswsFlag)
    library(faoswsImputation)
    library(magrittr)
    library(dplyr)
    ## TODO (Michael): The following packages should be removed when the
    ##                 \pkgs{faoswsProduction} package is ready and the module
    ##                 no longer depends on sourcing the R/ folder.
    ##
    ## library(faoswsProduction)
    library(data.table)
    library(splines)
})

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
savePath = paste0(R_SWS_SHARE_PATH, "/kao/production/imputation_fit/")
server = "QA"

if(CheckDebug()){
    cat("Not on server, so setting up environment...\n")

    stopifnot(server %in% c("QA", "Prod"))
    ## Define directories
    if(Sys.info()[7] == "josh"){
        apiDirectory = "~/Documents/Github/faoswsProduction/R/"
        R_SWS_SHARE_PATH = ifelse(server == "Prod", "/media/hqlprsws2_prod/",
                                  "/media/hqlprsws1_qa/")
        SetClientFiles(dir = ifelse(server == "Prod",
                                    "~/R certificate files/Production/",
                                    "~/R certificate files/QA/"))
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

## Get the required Datakey
completeImputationKey = getMainKey(years = imputationYears)
selectedItemCode = completeImputationKey@dimensions[["measuredItemCPC"]]@keys

for(iter in 1:length(selectedItemCode)){
    currentItem = selectedItemCode[iter]
    subKey = completeImputationKey
    subKey@dimensions$measuredItemCPC@keys = currentItem

    ## Obtain the formula and remove indigenous and biological meat.
    ##
    ## NOTE (Michael): Biological and indigenous meat are currently removed, as
    ##                 they have incorrect data specification. They should be
    ##                 separate item with different item code rather than under
    ##                 different element under the meat code.
    currentFormula =
        getYieldFormula(currentItem) %>%
        removeIndigenousBiologicalMeat(formula = .)

    ## Update the element corresponding to the current item
    subKey@dimensions$measuredElement@keys =
        currentFormula[, unlist(.(input, productivity, output))]

    ## Create processing parameters
    processingParams =
        defaultProcessingParameters(productionValue = currentFormula$output,
                                    areaHarvestedValue = currentFormula$input,
                                    yieldValue = currentFormula$productivity)

    print(paste0("Ensuring data consistency for item: ", currentItem, " (",
                 iter, " out of ", length(selectedItemCode),")"))

    ## NOTE (Michael): This is the data cleaning step, and official/semi-official
    ##                 flags can be over-written when in conflict are inconsistent.
    ##
    ## TODO (Michael): This should be probably be splitted into a seperate
    ##                 module.
    subKey %>%
        GetData(.) %>%
        preProcessing(data = .,
                      params = processingParams) %>%
        denormalise(normalisedData = .,
                    denormaliseKey = "measuredElement") %>%
        removeZeroYield(data = .,
                        yieldValue = processingParams$yieldValue,
                        yieldObsFlag = processingParams$yieldObservationFlag,
                        yieldMethodFlag = processingParams$yieldMethodFlag) %>%
        removeZeroConflict(data = .,
                           value1 = processingParams$productionValue,
                           value2 = processingParams$areaHarvestedValue,
                           observationFlag1 =
                               processingParams$productionObservationFlag,
                           observationFlag2 =
                               processingParams$areaHarvestedObservationFlag,
                           methodFlag1 = processingParams$productionMethodFlag,
                           methodFlag2 =
                               processingParams$areaHarvestedMethodFlag) %>%
        normalise(denormalisedData = .) %>%
        postProcessing(data = .,
                       params = processingParams) %>%
        SaveData(domain = "agriculture",
                 dataset = "aproduction",
                 data = .)



    ## Print message, initialise the save name and start the imputation.
    print(paste0("Imputation for item: ", currentItem, " (",  iter, " out of ",
                 length(selectedItemCode),")"))
    saveFileName = createImputationObjectName(item = currentItem)

    ## NOTE (Michael): We now impute the full triplet rather than
    ##                 just production for non-primary
    ##                 products. However, in the imputeModel
    ##                 module, only the production will be
    ##                 selected and imputed for non-primary
    ##                 products.
    ##
    ##                 Nevertheless, we hope to change this and
    ##                 impute the full triplet, for all the
    ##                 commodity. We will need to transfer and
    ##                 sync the inputs just like in the meat
    ##                 case. This will allow us to merge the two
    ##                 components.
    ##
    imputation = try({
        imputed = imputeMeatTriplet(meatKey = subKey)

        ## Check the imputation before saving.
        imputed %>%
            checkProductionBalanced(dataToBeSaved = .,
                                    areaVar = processingParams$areaHarvestedValue,
                                    yieldVar = processingParams$yieldValue,
                                    prodVar = processingParams$productionValue,
                                    conversion = currentFormula$unitConversion) %>%
            normalise(.) %>%
            postProcessing(data = .) %>%
            checkTimeSeriesImputed(dataToBeSaved = .,
                                   key = c("geographicAreaM49",
                                           "measuredItemCPC",
                                           "measuredElement"),
                                   valueColumn = "Value") %>%
            {
                ## Check whether protected data are being over-written
                filter(.data = ., flagMethod %in% c("i", "e")) %>%
                    checkProtectedData(dataToBeSaved = .)

                ## Save the fitted object for future loading
                postProcessing(data = ., params = processingParams) %>%
                    saveRDS(object = ., file = paste0(savePath, saveFileName))
            }

    })

    if(!inherits(imputation, "try-error")){
        message("Imputation module completed successfully")
    } else {
        stop(paste0("Imputation moduled failed at item ", currentItem))
    }
}


