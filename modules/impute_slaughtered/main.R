## ###########################################################################
## Title: Impute Slaughtered Module for SWS
##
## Author: Josh Browning
## Restructured : Michael C. J. Kao
##
## The animals slaughtered for production of meat, offals, fats and hides must
## be available before running the production imputation code.  These numbers,
## however, are not guaranteed to be available, and in the case of missing data,
## an imputation method must be applied.  The decision was to use the production
## figures of meat, if available, to compute the missing animals slaughtered. If
## these figures are also missing, they should be imputed using the production
## imputation methodology.  Of course, in the case of currently available data
## in the animal element, that data should be transferred to the quantity of
## animals slaughtered for meat and then the imputation ran.  We also decided to
## save the imputations for meat so as to retain consistency with the animal
## figures.
##
## The steps are as follows:
## 0. Transfer down the slaughtered animal numbers from the animal (parent)
## commodity to the meat (child) commodity.
## 1. Save the transferred data back to the database
## 2. Impute the meat data (production/animals slaughtered/carcass weight)
## following the logic from the production imputation module.
## 3. Copy the slaughtered animal numbers in meat back to the animal commodity.
## 4. Save all three variables for meat (production/animals slaughterd/carcass
## weight) and the animals slaughtered for the animal.
## #############################################################################

## Step 0. Initial set-up

cat("Beginning impute slaughtered script...\n")
suppressMessages({
    library(faosws)
    library(faoswsFlag)
    library(faoswsUtil)
    library(faoswsImputation)
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

("Loading preliminary data...\n")
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

## Load the animal/meat mapping table
selectedMeatTable =
    getAnimalMeatMapping(R_SWS_SHARE_PATH = R_SWS_SHARE_PATH,
                         onlyMeatChildren = TRUE) %>%
    select(measuredItemParentCPC, measuredElementParent,
           measuredItemChildCPC, measuredElementChild)

## Read the data.  The years and countries provided in the session are
## used, and the commodities in the session are somewhat
## considered. For example, if 02111 (Cattle) is in the session, then
## the session will be expanded to also include 21111.01 (meat of
## cattle, fresh or chilled), 21151 (edible offal of cattle, fresh,
## chilled or frozen), 21512 (cattle fat, unrendered), and 02951.01
## (raw hides and skins of cattle).  The measured element dimension of
## the session is simply ignored.

## Expand the session to include missing meats
expandedMeatKey =
    expandMeatSessionSelection(oldKey = sessionKey,
                               selectedMeatTable = selectedMeatTable)

## Adjust the years based on the passed information:
expandedMeatKey@dimensions[["timePointYears"]]@keys =
    as.character(imputationYears)

## Include all countries, since all data is required for the imputation
countryCodes = GetCodeList("agriculture", "aproduction", "geographicAreaM49")
expandedMeatKey@dimensions[["geographicAreaM49"]]@keys =
    countryCodes[type == "country", code]

## Execute the get data call.
cat("Pulling the complete data...\n")

## Transfer the animal number in the animal to the slaughtered animal in the
## meat.
animalTransferredData =
    expandedMeatKey %>%
    GetData(key = .) %>%
    checkFlagValidty(data = .) %>%
    checkProductionInputs(data = .) %>%
    fillRecord(data = .) %>%
    preProcessing(data = .) %>%
    transferAnimalNumber(data = ., selectedMeatTable)

## Module test and save the transferred data back
##
## NOTE (Michael): The transfer can over-write official and semi-official
##                 figures as indicated by in the synchronise slaughtered
##                 module.
cat("Saving the transferred animal to meat data back...\n")
animalTransferredData %>%
    postProcessing(data = .) %>%
    SaveData(expandedMeatKey@domain, expandedMeatKey@dataset, data = .)

## Step 2. Impute the meat data (production/animals
##         slaughtered/carcass weight) following the logic from the
##         production imputation module.
##

## NOTE (Michael): The imputed data for meat triplet is also saved
##                 back in this step.

cat("Imputing the meat commodity...\n")
selectedMeatCode =
    getSessionMeatSelection(key = expandedMeatKey,
                            selectedMeatTable = selectedMeatTable)


updatedSlaughteredAnimal = NULL
for(iter in seq(selectedMeatCode)){
    currentMeat = selectedMeatCode[iter]
    subKey = expandedMeatKey
    subKey@dimensions$measuredItemCPC@keys = currentMeat

    cat("Imputation for item: ", currentMeat, " (",  iter, " out of ",
        length(selectedMeatCode),")\n")


    ## Get all the formula associated with the item
    ##
    ## NOTE (Michael): Biological and indigenous meat are currently removed, as
    ##                 they have incorrect data specification. They should be
    ##                 separate item with different item code rather than under
    ##                 different element under the meat code.
    allFormula =
        getYieldFormula(itemCode = currentMeat) %>%
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

        ## Build imputation parameter
        imputationParameters =
            getImputationParameters(productionCode = currentFormula$output,
                                    areaHarvestedCode = currentFormula$input,
                                    yieldCode = currentFormula$productivity)

        ## Process the data.
        processedData =
            GetData(subKey) %>%
            checkFlagValidty(data = .) %>%
            checkProductionInputs(data = .) %>%
            fillRecord(data = .) %>%
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

        ## Perform module testing and save back to the database

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
            filter(flagMethod %in% c("i", "t", "e", "n", "u")) %>%
            ## NOTE (Michael): This test currently fails occasionally
            ##                 because it can not overwrite the ('I', '-')
            ##                 flag for imputed value from the old system.
            ##                 This is an error in the system as the flag
            ##                 ('I', '-') should be replaced with ('E', 'e')
            ##                 which can be over-written.
            checkProtectedData(dataToBeSaved = .) %>%
            SaveData(domain = subKey@domain,
                     dataset = subKey@dataset,
                     data = .)

        ## Extract the slaughtered number to be synced in the following step.
        ##
        ## NOTE (Michael): This function actually normalises data
        slaughteredAnimal =
            getSlaughteredAnimal(data = imputed,
                                 formulaTuples = currentFormula)
        updatedSlaughteredAnimal =
            rbind(updatedSlaughteredAnimal, slaughteredAnimal)
    }
}

if(!is.null(updatedSlaughteredAnimal)){
    cat("Saving back the updated animal numbers...\n")
    ## Step 3. Copy the slaughtered animal numbers in meat back to the
    ##         animal commodity.
    animalTransferredData %>%
        transferSlaughteredNumber(preUpdatedData = .,
                                  imputationResult = slaughteredAnimal,
                                  selectedMeatTable = selectedMeatTable) %>%
        ## Post process the data
        postProcessing(data = .) %>%
        ## Module Testing before saving the data back to the database
        checkTimeSeriesImputed(dataToBeSaved = .,
                               key = c("geographicAreaM49",
                                       "measuredItemCPC", "measuredElement"),
                               valueColumn = "Value") %>%
        checkProtectedData(dataToBeSaved = .) %>%
        ## Step 4. Save all three variables for meat (production/animals
        ##         slaughterd/carcass weight) and the animals slaughtered
        ##         for the animal.
        ##
        ## Note (Michael): The above comment is incorrect, only the animal
        ##                 number is saved back to the animal commdotiy.
        SaveData(domain = subKey@domain, dataset = subKey@dataset,
                 data = .)
    message("Imputation Module Executed Successfully!")
}



