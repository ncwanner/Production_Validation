## load the library
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

## Select the item list based on user input parameter
if(!imputationSelection %in% c("session", "all", "missing_items"))
    stop("Incorrect imputation selection specified")

##' Build processing parameters
processingParameters =
    productionProcessingParameters(datasetConfig = datasetConfig)


## Get the full imputation Datakey
completeImputationKey = getCompleteImputationKey()


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
nonLivestockImputationItems =
    getQueryKey("measuredItemCPC", completeImputationKey) %>%
    setdiff(., liveStockItems)

## These are the items selected by the users
sessionItems =
    getQueryKey("measuredItemCPC", sessionKey) %>%
    intersect(., nonLivestockImputationItems)

## This returns the list of items current does not have an imputed dataset.
missingItems =
    nonLivestockImputationItems[!imputationExist(savePath,
                                                 nonLivestockImputationItems)]

selectedItemCode =
    switch(imputationSelection,
           session = sessionItems,
           all = nonLivestockImputationItems,
           missing_items = missingItems)

for(iter in seq(selectedItemCode)){
    currentItem = selectedItemCode[iter]

    ## Obtain the formula and remove indigenous and biological meat.
    ##
    ## NOTE (Michael): Biological and indigenous meat are currently removed, as
    ##                 they have incorrect data specification. They should be
    ##                 separate item with different item code rather than under
    ##                 different element under the meat code.
    formulaTable =
        getYieldFormula(itemCode = currentItem) %>%
        removeIndigenousBiologicalMeat(formula = .)

    if(nrow(formulaTable) > 1)
        stop("Imputation should only use one formula")

    ## Create the formula parameter list
    formulaParameters =
        with(formulaTable,
             productionFormulaParameters(datasetConfig = datasetConfig,
                                         productionCode = output,
                                         areaHarvestedCode = input,
                                         yieldCode = productivity,
                                         unitConversion = unitConversion)
             )

    ## Update the item/element key according to the current commodity
    subKey = completeImputationKey
    subKey@dimensions$measuredItemCPC@keys = currentItem
    subKey@dimensions$measuredElement@keys =
        with(formulaParameters, c(productionCode, areaHarvestedCode, yieldCode))

    cat("Imputation for item: ", currentItem, " (",  iter, " out of ",
        length(selectedItemCode),")\n")


    ## Start the imputation
    ## Build imputation parameter
    imputationParameters =
        with(formulaParameters,
             getImputationParameters(productionCode = productionCode,
                                     areaHarvestedCode = areaHarvestedCode,
                                     yieldCode = yieldCode)
             )

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

    ## Process the data.
    processedData =
        GetData(subKey) %>%
        fillRecord(data = .) %>%
        preProcessing(data = .) %>%
        ensureProductionInputs(data = .,
                               processingParam = processingParameters,
                               formulaParameters = formulaParameters) %>%
        denormalise(normalisedData = .,
                    denormaliseKey = "measuredElement",
                    fillEmptyRecords = TRUE) %>%
        createTriplet(data = ., formula = meatFormulaTable) %>%
        processProductionDomain(data = .,
                                processingParameters = processingParameters,
                                formulaParameters = formulaParameters)

    ## Perform imputation
    imputed =
        imputeWithAndWithoutEstimates(
            data = processedData,
            processingParameters = processingParameters,
            formulaParameters = formulaParameters,
            imputationParameters = imputationParameters,
            minObsForEst = 5)

    ## Check the imputation before saving.
    imputed %>%
        normalise(.) %>%
        ensureProductionOutputs(data = .,
                                processingParameters = processingParameters,
                                formulaParameters = formulaParameters) %>%
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

