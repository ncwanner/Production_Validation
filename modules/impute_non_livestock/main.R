##' # Imputation of Non-Livestock Commodities
##'
##' **Author: Josh Browning, Michael C. J. Kao**
##'
##' **Description:**
##'
##' This module imputes the triplet (input, productivity and output) for a given
##' non-livestock commodity.
##'
##' **Inputs:**
##'
##' * Production domain
##' * Complete Key Table
##' * Livestock Element Mapping Table
##' * Identity Formula Table
##'
##' **Flag assignment:**
##'
##' | Procedure | Observation Status Flag | Method Flag|
##' | --- | --- | --- |
##' | Balance by Production Identity | `<flag aggregation>` | i |
##' | Imputation | I | e |
##'
##' **Data scope**
##'
##' * GeographicAreaM49: All countries specified in the `Complete Key Table`.
##'
##' * measuredItemCPC: Depends on the session selection. If the selection is
##'   "session", then only items selected in the session will be imputed. If the
##'   selection is "all", then all the items listed in the `Complete Key Table`
##'   excluding the live stock item in the `Livestock Element Mapping Table`
##'   will be imputed.
##'
##' * measuredElement: Depends on the measuredItemCPC, all cooresponding
##'   elements in the `Identity Formula Table`.
##'
##' * timePointYears: All years specified in the `Complete Key Table`.
##'
##' ---

## TODO (Michael): Merge the this module with the imputed slaughtered and
##                 synchronise slaughtered as the logic are identical.

##' ## Initialisation
##'

##' Load the libraries
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
    library(sendmailR)
})

##' Set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if(CheckDebug()){

    library(faoswsModules)
    SETTINGS = ReadSettings("modules/impute_non_livestock/sws.yml")

    ## If you're not on the system, your settings will overwrite any others
    R_SWS_SHARE_PATH = SETTINGS[["share"]]

    ## Define where your certificates are stored
    SetClientFiles(SETTINGS[["certdir"]])

    ## Get session information from SWS. Token must be obtained from web interface

    GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                       token = SETTINGS[["token"]])
}

##' Get user specified imputation selection
imputationSelection = swsContext.computationParams$imputation_selection

## imputationSelection= "session"


sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)

##' Check the validity of the computational parameter
if(!imputationSelection %in% c("session", "all"))
    stop("Incorrect imputation selection specified")

##' Get data configuration and session
sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)

##' Build processing parameters
processingParameters =
    productionProcessingParameters(datasetConfig = datasetConfig)

##' Get the full imputation Datakey
completeImputationKey = getCompleteImputationKey("production")


##' **NOTE (Michael): Since the animal/meat are currently imputed by the
##'                   imputed_slaughtered and synchronise slaughtered module, so
##'                   they should be excluded here.**
##'
liveStockItems =
    getAnimalMeatMapping(R_SWS_SHARE_PATH = R_SWS_SHARE_PATH,
                         onlyMeatChildren = FALSE) %>%
    select(measuredItemParentCPC, measuredItemChildCPC) %>%
    unlist(x = ., use.names = FALSE) %>%
    unique(x = .)


##' This is the complete list of items that are in the imputation list
nonLivestockImputationItems =
    getQueryKey("measuredItemCPC", completeImputationKey) %>%
    setdiff(., liveStockItems)

##' These are the items selected by the users
sessionItems =
    getQueryKey("measuredItemCPC", sessionKey) %>%
    intersect(., nonLivestockImputationItems)

##' Select the commodities based on the user input parameter
selectedItemCode =
    switch(imputationSelection,
           "session" = sessionItems,
           "all" = nonLivestockImputationItems)

##' ---
##' ## Perform Imputation
imputationResult = data.table()

##' Loop through the commodities to impute the items individually.
for(iter in seq(selectedItemCode)){
    imputationProcess =
        try({
            set.seed(070416)
            
            currentItem = selectedItemCode[iter]

            ## Obtain the formula and remove indigenous and biological meat.
            ##
            ## NOTE (Michael): Biological and indigenous meat are currently
            ##                 removed, as they have incorrect data
            ##                 specification. They should be separate item with
            ##                 different item code rather than under different
            ##                 element under the meat code.
            formulaTable =
                getProductionFormula(itemCode = currentItem) %>%
                removeIndigenousBiologicalMeat(formula = .)

            ## NOTE (Michael): Imputation should be performed on only 1 formula,
            ##                 if there are multiple formulas, they should be
            ##                 calculated based on the values imputed. For
            ##                 example, if one of the formula has production in
            ##                 tonnes while the other has production in
            ##                 kilo-gram, then we should impute the production
            ##                 in tonnes, then calculate the production in
            ##                 kilo-gram.
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
                with(formulaParameters,
                     c(productionCode, areaHarvestedCode, yieldCode))

            ## Start the imputation
            message("Imputation for item: ", currentItem, " (",  iter, " out of ",
                    length(selectedItemCode),")")

            ## Build imputation parameter
            imputationParameters =
                with(formulaParameters,
                     getImputationParameters(productionCode = productionCode,
                                             areaHarvestedCode = areaHarvestedCode,
                                             yieldCode = yieldCode)
                     )

            ## Extract the data, and skip the imputation if the data contains no entry.
            extractedData =
                GetData(subKey)

            if(nrow(extractedData) == 0){
                message("Item : ", currentItem, " does not contain any data")
                next
            }

            ## Process the data.
            processedData =
                extractedData %>%
                preProcessing(data = .) %>%
                expandYear(data = .,
                           areaVar = processingParameters$areaVar,
                           elementVar = processingParameters$elementVar,
                           itemVar = processingParameters$itemVar,
                           valueVar = processingParameters$valueVar) %>%
                denormalise(normalisedData = .,
                            denormaliseKey = "measuredElement",
                            fillEmptyRecords = TRUE) %>%
                createTriplet(data = ., formula = formulaTable) %>%
                processProductionDomain(data = .,
                                        processingParameters =
                                            processingParameters,
                                        formulaParameters = formulaParameters) %>%
                ensureProductionInputs(data = .,
                                       processingParam = processingParameters,
                                       formulaParameters = formulaParameters,
                                       normalised = FALSE)

            ## Perform imputation
            imputed =
                imputeProductionTriplet(
                    data = processedData,
                    processingParameters = processingParameters,
                    formulaParameters = formulaParameters,
                    imputationParameters = imputationParameters)


            ## Save the imputation back to the database.
            imputed %>%
                ## NOTE (Michael): Records containing invalid dates are
                ##                 excluded, for example, South Sudan only came
                ##                 into existence in 2011. Thus although we can
                ##                 impute it, they should not be saved back to
                ##                 the database.
                removeInvalidDates(data = ., context = sessionKey) %>%
                mutate(timePointYears = as.character(timePointYears)) %>%
                ensureProductionOutputs(data = .,
                                        processingParameters =
                                            processingParameters,
                                        formulaParameters = formulaParameters,
                                        normalised = FALSE) %>%
                normalise(.) %>%
                ## NOTE (Michael): Only data with method flag "i" for balanced,
                ##                 or flag combination (I, e) for imputed are
                ##                 saved back to the database.
                filter(., flagMethod == "i" |
                          (flagObservationStatus == "I" &
                           flagMethod == "e")) %>%
                ensureProtectedData(data = .,
                                    domain = sessionKey@domain,
                                    dataset = sessionKey@dataset) %>%
                postProcessing(data = .) %>%
                SaveData(domain = sessionKey@domain,
                         dataset = sessionKey@dataset,
                         data = .)

        })

    ## Capture the items that failed
    if(inherits(imputationProcess, "try-error"))
        imputationResult =
            rbind(imputationResult,
                  data.table(item = currentItem,
                             error = imputationProcess[1]))

}

##' ---
##' ## Return Message

if(nrow(imputationResult) > 0){
    ## Initiate email
    from = "I_am_a_magical_unicorn@sebastian_quotes.com"
    to = swsContext.userEmail
    subject = "Imputation Result"
    body = paste0("The following items failed, please inform the maintainer "
                  , "of the module")

    errorAttachmentName = "non_livestock_imputation_result.csv"
    errorAttachmentPath =
        paste0(R_SWS_SHARE_PATH, "/kao/", errorAttachmentName)
    write.csv(imputationResult, file = errorAttachmentPath,
              row.names = FALSE)
    errorAttachmentObject = mime_part(x = errorAttachmentPath,
                                      name = errorAttachmentName)

    bodyWithAttachment = list(body, errorAttachmentObject)

    sendmail(from = from, to = to, subject = subject, msg = bodyWithAttachment)
    stop("Production imputation incomplete, check following email to see where ",
         " it failed")
} else {
    msg = "Imputation Completed Successfully"
    message(msg)
    msg
}
