########################################################################
## Module to balance the production domain identity. That is the equation
## Production = Area * yield
## Author: Michael C. J. Kao
########################################################################

suppressMessages({
    library(faosws)
    library(faoswsFlag)
    library(faoswsUtil)
    library(faoswsProduction)
    library(magrittr)
    library(dplyr)
})

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")


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
}

startTime = Sys.time()

## Variable to determine if all yield data should be computed (across entire
## database) or just local session.
updateAllData =
    !is.null(swsContext.computationParams$computation_selection) &&
    swsContext.computationParams$computation_selection == "all"

newKey = swsContext.datasets[[1]]
## If all yields should be updated, extend the key
if(updateAllData){
    newKey = getAllYieldKey()
}

## Create the formula table
##
## NOTE (Michael): There are differemt formulas for different
##                 commodities, so we loop through multiple formulas
##                 to ensure all formulas are computed.
formulaTuples =
    getYieldFormula(slot(slot(swsContext.datasets[[1]],
                              "dimensions")$measuredItemCPC, "keys"),
                    warn = TRUE)
unique_formulas = unique(formulaTuples[, list(input, productivity, output,
                                              unitConversion)])

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

    ## Get the yield data and perform the necessary pre-processing
    yieldData =
        getYieldData(subKey) %>%
        .$query %>%
        fillRecord(data = .) %>%
        normalise(denormalisedData = .) %>%
        preProcessing(data = .) %>%
        denormalise(normalisedData = ., denormaliseKey = "measuredElement")

    ## Obtain the processing parameter associated with the data
    processingParams = defaultProcessingParameters(
        productionValue = current_formula[, output],
        yieldValue = current_formula[, productivity],
        areaHarvestedValue = current_formula[, input])

    ## Perform the yield module.
    computeYield(data = yieldData,
                 processingParameters = processingParams,
                 unitConversion = current_formula$unitConversion)
    balanceProduction(data = yieldData,
                      processingParameters = processingParams,
                      unitConversion = current_formula$unitConversion)
    balanceAreaHarvested(data = yieldData,
                         processingParameters = processingParams,
                         unitConversion = current_formula$unitConversion)
    ## Module testing
    cat("Module Testing and saving data back ... \n")

    yieldData %>%
        checkProductionBalanced(dataToBeSaved = .,
                                areaVar = processingParams$areaHarvestedValue,
                                yieldVar = processingParams$yieldValue,
                                prodVar = processingParams$productionValue,
                                conversion = processingParams$unitConversion) %>%
        checkIdentityCalculated(dataToBeSaved = .,
                                areaVar = processingParams$areaHarvestedValue,
                                yieldVar = processingParams$yieldValue,
                                prodVar = processingParams$productionValue) %>%
        normalise(.) %>%
        postProcessing(.) %>%
        filter(filter = flagMethod %in% c("i", "t", "e", "n", "u")) %>%
        checkProtectedData(dataToBeSaved = .) %>%
        SaveData(domain = "agriculture",
                 dataset = "aproduction",
                 data = .)

}


paste("Module completed in",
      round(difftime(Sys.time(), startTime, units = "min"), 2), "minutes.")
