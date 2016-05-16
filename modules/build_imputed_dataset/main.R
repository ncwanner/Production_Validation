## load the library
suppressMessages({
    library(faosws)
    library(faoswsUtil)
    library(faoswsFlag)
    library(faoswsImputation)
    library(faoswsProduction)
    library(magrittr)
    library(dplyr)
})

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
savePath = paste0(R_SWS_SHARE_PATH, "/kao/production/imputation_fit/")

## This return FALSE if on the Statistical Working System
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

## Get user specified imputation selection
imputationSelection = swsContext.computationParams$imputation_selection
sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)


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

## Get the full imputation Datakey
completeImputationKey = getMainKey(years = imputationYears)


## NOTE (Michael): Since the animal/meat are currently imputed by the
##                 imputed_slaughtered and synchronise slaughtered module, so
##                 they should not be imputed here.
##
## TODO (Michael): Merge the this module with the imputed slaughtered and
##                 synchronise slaughtered as the logic are identical.
liveStockItems =
    getAnimalMeatMapping(R_SWS_SHARE_PATH = R_SWS_SHARE_PATH,
                         onlyMeatChildren = FALSE) %>%
    select(measuredItemParentCPC, measuredItemChildCPC) %>%
    unlist(x = ., use.names = FALSE) %>%
    unique(x = .)


## This is the complete list of items that are in the imputation list
completeImputationItems =
    completeImputationKey@dimensions[["measuredItemCPC"]]@keys %>%
    setdiff(., liveStockItems)

## These are the items selected by the users
sessionItems =
    intersect(sessionKey@dimensions[["measuredItemCPC"]]@keys,
              completeImputationItems)

## This returns the list of items current does not have an imputed dataset.
missingItems =
    completeImputationItems[!imputationExist(savePath, completeImputationItems)]

## Select the item list based on user input parameter
if(!imputationSelection %in% c("session", "all", "missing_items"))
    stop("Incorrect imputation selection specified")

selectedItemCode =
    switch(imputationSelection,
           session = sessionItems,
           all = completeImputationItems,
           missing_items = missingItems)

for(iter in seq(selectedItemCode)){
    currentItem = selectedItemCode[iter]
    subKey = completeImputationKey
    subKey@dimensions$measuredItemCPC@keys = currentItem

    cat("Imputation for item: ", currentItem, " (",  iter, " out of ",
        length(selectedItemCode),")\n")

    ## Obtain the formula and remove indigenous and biological meat.
    ##
    ## NOTE (Michael): Biological and indigenous meat are currently removed, as
    ##                 they have incorrect data specification. They should be
    ##                 separate item with different item code rather than under
    ##                 different element under the meat code.
    allFormula =
        getYieldFormula(itemCode = currentItem) %>%
        removeIndigenousBiologicalMeat(formula = .)

    for(j in 1:nrow(allFormula)){
        currentFormula = allFormula[j, ]
        with(currentFormula,
             cat("Imputation for formula: ", output, " = ", input, " x ",
                 productivity, " (", j, " out of ", nrow(allFormula), ")\n"))

        ## Update the element code according to the current formula
        subKey@dimensions$measuredElement@keys =
            currentFormula[, unlist(.(input, productivity, output))]

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

        ## Build imputation parameter
        imputationParameters =
            getImputationParameters(productionCode = currentFormula$output,
                                    areaHarvestedCode = currentFormula$input,
                                    yieldCode = currentFormula$productivity)

        ## Process the data.
        processedData =
            GetData(subKey) %>%
            fillRecord(data = .) %>%
            checkFlagValidity(data = .) %>%
            checkProductionInputs(data = .) %>%
            preProcessing(data = .) %>%
            denormalise(normalisedData = ., denormaliseKey = "measuredElement") %>%
            processProductionDomain(data = .,
                                    processingParameters = processingParameters)

        ## Perform imputation
        imputed =
            imputeWithAndWithoutEstimates(
                data = processedData,
                processingParameters = processingParameters,
                imputationParameters = imputationParameters,
                minObsForEst = 5)

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
            postProcessing(data = .) %>%
            {
                ## HACK (Michael): Before we decide how to deal with the flags,
                ##                 we will not perform this check as we can not
                ##                 determine what is considered 'protected'
                ##
                ## Check whether protected data are being over-written
                ## filter(flagMethod %in% c("i", "t", "e", "n", "u")) %>%
                ##     checkProtectedData(dataToBeSaved = .) %>%
                ##     print(.)

                ## Save the fitted object for future loading
                saveRDS(object = ., file = paste0(savePath, saveFileName))
            }

    }
}

