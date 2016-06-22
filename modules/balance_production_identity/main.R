##' # Balance Production Identity
##'
##' **Author: Michael C. J. Kao**
##'
##' **Descriptions:**
##'
##' Module to balance the production domain identity. That is the equation
##' Production = Area x yield
##'
##' **Inputs:**
##'
##' * Production domain
##' * Identity Formula Table
##'
##' **Steps:**
##'
##' 1. Compute Yield
##' 2. Balance Production
##' 3. Balance Area Harvested
##'
##' **Flag Changes:**
##'
##' | Procedure | Observation Status Flag | Method Flag|
##' | --- | --- | --- |
##' | Compute/Balance | `<Flag Aggregation>` | i |
##'
##' ---


##' ## Initialisation

##' Load required libraries
suppressMessages({
    library(faosws)
    library(faoswsFlag)
    library(faoswsUtil)
    library(faoswsProduction)
    library(faoswsProcessing)
    library(faoswsEnsure)
    library(magrittr)
    library(dplyr)
})

##' Set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")


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
}

startTime = Sys.time()

##' Variable to determine if all yield data should be computed (across entire
##' database) or just local session.
updateAllData =
    !is.null(swsContext.computationParams$computation_selection) &&
    swsContext.computationParams$computation_selection == "all"

datasetConfig = GetDatasetConfig("agriculture", "aproduction")

sessionKey = swsContext.datasets[[1]]
##' If all yields should be updated, extend the key
if(updateAllData){
    sessionKey = getAllYieldKey()
}

##' Create the formula table
##'
##' NOTE (Michael): There are differemt formulas for different
##'                 commodities, so we loop through multiple formulas
##'                 to ensure all formulas are computed.
formulaTuples =
    getQueryKey("measuredItemCPC", sessionKey) %>%
    getProductionFormula(itemCode = ., warn = TRUE)

unique_formulas = unique(formulaTuples[, list(input, productivity, output,
                                              unitConversion)])


##' ---
##' ## Balance Production Identity

##' Loop through the formulas
for(i in seq(nrow(unique_formulas))){
    ## Subset the formula table
    current_formula = unique_formulas[i, ]
    with(current_formula,
         cat("Updating formula: ", output, " = ", input, " * ", productivity,
             "(", i, " out of ", nrow(unique_formulas), ")\n")
         )

    ## Get all the CPC codes we need by merging the specific
    ## production/output/input codes with the dataset.
    currentCPC = merge(formulaTuples, current_formula,
                       by = c("input", "productivity", "output",
                              "unitConversion"))[, measuredItemCPC]

    ## Create a subset key and filter the context to just the revelant
    ## item/element keys
    subKey = sessionKey
    subKey@dimensions$measuredElement@keys =
        as.character(current_formula[, list(input, output, productivity)])
    subKey@dimensions$measuredItemCPC@keys = currentCPC

    ## Obtain the processing parameter associated with the data
    processingParams =
        productionProcessingParameters(datasetConfig = datasetConfig)


    ## Create the formula parameter list
    formulaParameters =
        with(current_formula,
             productionFormulaParameters(datasetConfig = datasetConfig,
                                         productionCode = output,
                                         areaHarvestedCode = input,
                                         yieldCode = productivity,
                                         unitConversion = unitConversion)
             )

    ## Get the yield data and perform the necessary pre-processing
    ##
    ## NOTE (Michael): Should we also remove all previously calculated data?
    yieldData =
        GetData(subKey) %>%
        preProcessing(data = .) %>%
        denormalise(normalisedData = .,
                    denormaliseKey = "measuredElement") %>%
        createTriplet(data = .,
                      formula = current_formula) %>%
        ensureProductionInputs(data = .,
                               processingParam = processingParams,
                               formulaParameters = formulaParameters,
                               normalised = FALSE)

    ## Perform the yield module.
    ##
    ## NOTE (Michael): Maybe should have a while loop for this.
    computeYield(data = yieldData,
                 processingParameters = processingParams,
                 formulaParameters = formulaParameters)
    balanceProduction(data = yieldData,
                      processingParameters = processingParams,
                      formulaParameters = formulaParameters)
    balanceAreaHarvested(data = yieldData,
                         processingParameters = processingParams,
                         formulaParameters = formulaParameters)

    ## Module testing
    cat("Module Testing and saving data back ... \n")

    yieldData %>%
        normalise(.) %>%
        postProcessing(.) %>%
        filter(filter = flagMethod == "i") %>%
        ensureProductionOutputs(data = .,
                                processingParameters = processingParams,
                                formulaParameters = formulaParameters) %>%
        SaveData(domain = sessionKey@domain,
                 dataset = sessionKey@dataset,
                 data = .)

}

##' ---
##' Return Message

paste("Module completed in",
      round(difftime(Sys.time(), startTime, units = "min"), 2), "minutes.")
