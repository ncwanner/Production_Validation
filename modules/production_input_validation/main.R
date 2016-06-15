##' # Production Input Validation Module
##'
##' **Desscription:**
##'
##' This module examins whether the production data are valid.
##'
##' **Author: Michael C. J. Kao**
##'
##' **Validation:**
##'
##' Currently there are 5 tests in the suite:
##'
##' 1. Check whether flags are valid.
##' 2. Check whether missing values are specified correctly
##' 3. Check whether the production (input) values are within feasible range.
##' 4. Check whether the area harvested (output) values are within feasible range.
##' 5. Check whether the yield (productivity) values are within feasible range.
##' 6. Check whether the production triplet (input, output productivity) are
##'    balanced.
##'
##' **Auto-corrections:**
##'
##' 1. Flag (M, -) --> (M, u)
##' 2. Flag (E, t) --> (E, -)
##' 3. Flag (E, e) --> (I, e)
##' 4. Flag (E, p) --> (E, f)
##' 5. Conflicting area harvested and production are removed.
##' 6. Zero yield are removed.
##'
##' **Steps:**
##'
##' 1. Perform auto-correction
##' 2. Remove previous production processing.
##' 3. Perform validation on non-processed data
##' 4. Save auto-corrected data back to the database
##' 5. Send email to user if input data contains invalid entries.
##'
##' ---


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

##' Load the configuration if in debug mode
if(CheckDebug()){

    library(faoswsModules)
    SETTINGS = ReadSettings("sws.yml")

    R_SWS_SHARE_PATH = SETTINGS[["share"]]

    ## Define where your certificates are stored
    SetClientFiles(SETTINGS[["certdir"]])

    GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                       token = SETTINGS[["token"]])

}

##' Obtain computation parameter, this parameter determines whether only
##' selected session should be validated or the complete production domain.
validationRange = swsContext.computationParams$validation_selection

##' Get session key and dataset configuration
sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)


##' Obtain the complete imputation Datakey
completeImputationKey = getCompleteImputationKey("production")

##' Selected the key based on the input parameter
selectedKey =
    switch(validationRange,
           "session" = sessionKey,
           "all" = completeImputationKey)


##' Build processing parameters, the processing parameters contain parameters on
##' how the data should be processed.
processingParameters =
    productionProcessingParameters(datasetConfig = datasetConfig)



##' ---
##' ## Define auto-correction.
##'
##' Certain invalid data can be corrected if a correction rule is agreed a
##' priori. Both function should be removed from the module, possibly to the
##' ensure package.
##'

##' Function to perform flag correction
autoFlagCorrection = function(data,
                              flagObservationStatusVar = "flagObservationtatus",
                              flagMethodVar = "flagMethod"){
    dataCopy = copy(data)

    ## Correction (1): (M, -) --> (M, u)
    correctionFilter =
        dataCopy[[flagObservationStatusVar]] == "M" &
        dataCopy[[flagMethodVar]] == "-"
    dataCopy[correctionFilter,
             `:=`(c(flagObservationStatusVar, flagMethodVar),
                  c("M", "u"))]

    ## Correction (2): (E, t) --> (E, -)
    correctionFilter =
        dataCopy[[flagObservationStatusVar]] == "E" &
        dataCopy[[flagMethodVar]] == "t"
    dataCopy[correctionFilter,
             `:=`(c(flagObservationStatusVar, flagMethodVar),
                  c("E", "-"))]

    ## Correction (3): (E, e) --> (I, e)
    correctionFilter =
        dataCopy[[flagObservationStatusVar]] == "E" &
        dataCopy[[flagMethodVar]] == "e"
    dataCopy[correctionFilter,
             `:=`(c(flagObservationStatusVar, flagMethodVar),
                  c("I", "e"))]

    ## Correction (4): (E, p) --> (E, f)
    correctionFilter =
        dataCopy[[flagObservationStatusVar]] == "E" &
        dataCopy[[flagMethodVar]] == "p"
    dataCopy[correctionFilter,
             `:=`(c(flagObservationStatusVar, flagMethodVar),
                  c("E", "f"))]

    dataCopy
}

##' Function to perform value correction
autoValueCorrection = function(data,
                               processingParameters,
                               formulaParameters){
    correctedData =
        with(formulaParameters,
             with(processingParameters,
                  data %>%
                  removeZeroConflict(data = .,
                                     value1 = productionValue,
                                     value2 = areaHarvestedValue,
                                     observationFlag1 = productionObservationFlag,
                                     observationFlag2 =
                                         areaHarvestedObservationFlag,
                                     methodFlag1 = productionMethodFlag,
                                     methodFlag2 = areaHarvestedMethodFlag,
                                     missingObservationFlag =
                                         missingValueObservationFlag,
                                     missingMethodFlag =
                                         missingValueMethodFlag) %>%
                  removeZeroYield(data = .,
                                  yieldValue = yieldValue,
                                  yieldObsFlag = yieldObservationFlag,
                                  yieldMethodFlag = yieldMethodFlag)
                  )
             )
    correctedData
}

##' ## Perform Validation
##'

##' Extract the selected item code which will be looped over, we need to loop
##' over items as different item can have different formula. As we need to check
##' production is balanced, we need to substitute the correct formula for each
##' commodity.
selectedItem = getQueryKey("measuredItemCPC", selectedKey)

##' The tests and test messages
tests = c("flag_validity", "correct_missing_value",
          "production_range", "areaHarvested_range", "yield_range", "balanced")

##' Initialise the error list
errorList = vector("list", length(tests))

completeData =
    GetData(selectedKey) %>%
    preProcessing(data = .)

##' Check the flags
errorList[[1]] =
    completeData %>%
    ensureFlagValidity(data = .,
                       getInvalidData = TRUE)

##' Ensuring missing values are correctly specified
errorList[[2]] =
    completeData %>%
    ensureCorrectMissingValue(data = .,
                              valueVar = "Value",
                              flagObservationStatusVar = "flagObservationStatus",
                              missingObservationFlag = "M",
                              returnData = FALSE,
                              getInvalidData = TRUE)

##' Perform input validation item by item
for(iter in seq(selectedItem)){

    currentItem = selectedItem[iter]
    message("Performing input validation for item: ", currentItem)

    ## Get the formula (elements) associated with the current item
    currentFormula =
        getProductionFormula(currentItem) %>%
        removeIndigenousBiologicalMeat
    currentElements = with(currentFormula, c(input, productivity, output))

    ## Update the item and element key to for current item
    currentKey = selectedKey
    currentKey@dimensions$measuredItemCPC@keys = currentItem
    currentKey@dimensions$measuredElement@keys = currentElements

    ## Create the formula parameter list
    formulaParameters =
        with(currentFormula,
             productionFormulaParameters(datasetConfig = datasetConfig,
                                         productionCode = output,
                                         areaHarvestedCode = input,
                                         yieldCode = productivity,
                                         unitConversion = unitConversion))

    ## Extract the current data
    currentData =
        currentKey %>%
        GetData(key = .)

    if(nrow(currentData) > 0){
        ## NOTE (Michael): The test should be conducted on the raw data, that is
        ##                 excluding any imputation, statistical estimation and
        ##                 previous calculated values. However, auto corrected
        ##                 values will be saved back to the database.

        ## Correct the flag and values here where available based on agreed
        ## correction rules.
        autoCorrectedData =
            currentData %>%
            preProcessing(data = .) %>%
            autoFlagCorrection(data = .) %>%
            denormalise(normalisedData = .,
                        denormaliseKey = processingParameters$elementVar) %>%
            createTriplet(data = ., formula = currentFormula) %>%
            autoValueCorrection(data = .,
                                processingParameters = processingParameters,
                                formulaParameters = formulaParameters)

        ## Remove all values generated by previous production process (e.g.
        ## previous imputation/calculation) and also manual estimates.
        rawData =
            autoCorrectedData %>%
            processProductionDomain(data = .,
                                    processingParameters = processingParameters,
                                    formulaParameters = formulaParameters)


        ## Check production value
        errorList[[3]] =
            with(formulaParameters,
            {
                rawData %>%
                    ensureValueRange(data = .,
                                     ensureColumn = productionValue,
                                     getInvalidData = TRUE) %>%
                    normalise %>%
                    rbind(errorList[[3]], .)
            }
            )

        ## Check area harvested value
        errorList[[4]] =
            with(formulaParameters,
            {
                rawData %>%
                    ensureValueRange(data = .,
                                     ensureColumn = areaHarvestedValue,
                                     getInvalidData = TRUE) %>%
                    normalise %>%
                    rbind(errorList[[4]], .)
            }
            )

        ## Check yield value
        errorList[[5]] =
            with(formulaParameters,
            {
                rawData %>%
                    ensureValueRange(data = .,
                                     ensureColumn = yieldValue,
                                     getInvalidData = TRUE,
                                     includeEndPoint = FALSE) %>%
                    normalise %>%
                    rbind(errorList[[5]], .)
            }
            )

        ## Check production domain balanced.
        errorList[[6]] =
            with(formulaParameters,
            {
                rawData %>%
                    ensureProductionBalanced(data = .,
                                             areaVar = areaHarvestedValue,
                                             yieldVar = yieldValue,
                                             prodVar = productionValue,
                                             conversion = unitConversion,
                                             getInvalidData = TRUE,
                                             normalised = FALSE) %>%
                    normalise %>%
                    rbind(errorList[[6]], .)
            }
            )

        ## Save the auto-corrected data back
        with(formulaParameters,
        {
            autoCorrectedData %>%
                ensureValueRange(data = .,
                                 ensureColumn = productionValue,
                                 getInvalidData = TRUE) %>%
                ensureValueRange(data = .,
                                 ensureColumn = areaHarvestedValue,
                                 getInvalidData = TRUE) %>%
                ensureValueRange(data = .,
                                 ensureColumn = yieldValue,
                                 getInvalidData = TRUE) %>%
                ensureProductionBalanced(data = .,
                                         areaVar = areaHarvestedValue,
                                         yieldVar = yieldValue,
                                         prodVar = productionValue,
                                         conversion = unitConversion,
                                         getInvalidData = TRUE,
                                         normalised = FALSE) %>%
                normalise %>%
                postProcessing %>%
                ensureFlagValidity(data = .,
                                   getInvalidData = TRUE) %>%
                SaveData(domain = sessionKey@domain,
                         dataset = sessionKey@dataset,
                         data = .)
        })

    } else {
        message("Current Item has no data")
    }

}



##' ---
##' ## Return Messages and Send Error
##'

##' Send email to user if the data contains invalid entries.
if(max(sapply(errorList, nrow)) > 0){

    ## Initiate email
    from = "I_am_a_magical_unicorn@sebastian_quotes.com"
    to = swsContext.userEmail
    subject = "Validation Result"
    body = paste0("There are 5 tests current in the system:\n",
                  "1. Flag validity: Whether a flag is valid\n",
                  "2. Missing values are correctly specified\n",
                  "3. Production range: [0, Inf)\n",
                  "4. Area Harvested range: [0, Inf)\n",
                  "5. Yield range: (0, Inf)\n",
                  "6. Production balanced")


    ## Function to attach error to email
    createErrorAttachmentObject = function(testName,
                                           testResult,
                                           R_SWS_SHARE_PATH){
        errorAttachmentName = paste0(testName, ".csv")
        errorAttachmentPath =
            paste0(R_SWS_SHARE_PATH, "/kao/", errorAttachmentName)
        write.csv(testResult, file = errorAttachmentPath,
                  row.names = FALSE)
        errorAttachmentObject = mime_part(x = errorAttachmentPath,
                                          name = errorAttachmentName)
       errorAttachmentObject
    }

    ## Create attachment for errors
    testContainInvalidData =
        which(sapply(errorList, nrow) > 0)

    bodyWithAttachment =
        vector("list", length = length(testContainInvalidData) + 1)
    bodyWithAttachment[[1]] = body

    for(i in seq(length(testContainInvalidData))){
        failedIndex = testContainInvalidData[i]
        bodyWithAttachment[[(i + 1)]] =
            createErrorAttachmentObject(tests[[failedIndex]],
                                        errorList[[failedIndex]],
                                        R_SWS_SHARE_PATH)
    }

    ## Send the email
    sendmail(from = from, to = to, subject = subject, msg = bodyWithAttachment)
    stop("Production Input Invalid, please check follow up email on invalid data")
} else {
    msg = "Production Input Validation passed without any error!"
    message(msg)
    msg
}


