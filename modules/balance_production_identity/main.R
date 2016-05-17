##' # Balance Production Identity
##'
##' Module to balance the production domain identity. That is the equation
##' Production = Area x yield
##'
##' Author: Michael C. J. Kao
##'
##' ---

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

newKey = swsContext.datasets[[1]]
##' If all yields should be updated, extend the key
if(updateAllData){
    newKey = getAllYieldKey()
}

##' Create the formula table
##'
##' NOTE (Michael): There are differemt formulas for different
##'                 commodities, so we loop through multiple formulas
##'                 to ensure all formulas are computed.
formulaTuples =
    getYieldFormula(slot(slot(swsContext.datasets[[1]],
                              "dimensions")$measuredItemCPC, "keys"),
                    warn = TRUE)
unique_formulas = unique(formulaTuples[, list(input, productivity, output,
                                              unitConversion)])

##' Loop through the formulas
for(i in 1:nrow(unique_formulas)){
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
    subKey = newKey
    subKey@dimensions$measuredElement@keys =
        as.character(current_formula[, list(input, output, productivity)])
    subKey@dimensions$measuredItemCPC@keys = currentCPC

    ## Obtain the processing parameter associated with the data
    processingParams =
        productionProcessingParameters(
            datasetConfig = GetDatasetConfig("agriculture", "aproduction"),
            productionCode = current_formula[, output],
            yieldCode = current_formula[, productivity],
            areaHarvestedCode = current_formula[, input])

    ## Get the yield data and perform the necessary pre-processing
    yieldData =
        getYieldData(subKey) %>%
        .$query %>%
        fillRecord(data = .) %>%
        ensureProductionInputs(data = .,
                               processingParam = processingParams,
                               normalised = FALSE) %>%
        preProcessing(data = .,
                      normalised = FALSE) %>%
        removeZeroYield(data = .,
                        yieldValue = processingParams$yieldValue,
                        yieldObsFlag =
                            processingParams$yieldObservationFlag,
                        yieldMethodFlag = processingParams$yieldMethodFlag)

    ## Perform the yield module.
    computeYield(data = yieldData,
                 processingParameters = processingParams)
    balanceProduction(data = yieldData,
                      processingParameters = processingParams)
    balanceAreaHarvested(data = yieldData,
                         processingParameters = processingParams)
    ## Module testing
    cat("Module Testing and saving data back ... \n")

    yieldData %>%
        normalise(.) %>%
        postProcessing(.) %>%
        filter(filter = flagMethod %in% c("i", "t", "e", "n", "u")) %>%
        ensureProductionOutputs(data = .,
                                processingParameters = processingParams) %>%
        SaveData(domain = "agriculture",
                 dataset = "aproduction",
                 data = .)

}

##' Return results
paste("Module completed in",
      round(difftime(Sys.time(), startTime, units = "min"), 2), "minutes.")
