##' # This module examins whether the production data are valid.
##'
##' **Author: Michael C. J. Kao**
##'
##' Currently there are 5 tests in the suite:
##'
##' 1. Check whether flags are valid.
##' 2. Check whether the production (input) values are within feasible range.
##' 3. Check whether the area harvested (output) values are within feasible range.
##' 4. Check whether the yield (productivity) values are within feasible range.
##' 5. check whether the production triplet (input, output productivity) are
##'    balanced.

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

##' set up for the test environment and parameters
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

##' Obtain computation parameter
validationRange = swsContext.computationParams$validation_selection

##' Get session key and dataset configuration
sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)


##' Obtain the complete imputation Datakey
completeImputationKey = getCompleteImputationKey()

##' Selected the key based on the input parameter
selectedKey =
    switch(validationRange,
           "session" = sessionKey,
           "all" = completeImputationKey)


##' Build processing parameters
processingParameters =
    productionProcessingParameters(datasetConfig = datasetConfig)


##' Perform autocorrection, certain invalid data can be corrected if a
##' correction rule is agreed a priori.

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


##' Extract the selected item code which will be looped over
selectedItem =
    unique(slot(slot(selectedKey, "dimensions")[[processingParameters$itemVar]],
                "keys"))

##' The tests and test messages
tests = c("flag_validity", "production_range", "areaHarvested_range",
          "yield_range", "balanced")
testMessage = c("The following entry contains invalid flag",
                "The following entry contain invalid production value",
                "The following entry contain invalid area harvsted value",
                "The following entry contain invalid yield value",
                "The following entry is imbalanced, check the values then re-run balance production identity module")

##' Initialise the error list
errorList = vector("list", length(tests))


##' Perform input validation item by item
for(iter in seq(selectedItem)){

    currentItem = selectedItem[iter]
    message("Performing input validation for item: ", currentItem)
    currentFormula =
        getYieldFormula(currentItem) %>%
        removeIndigenousBiologicalMeat
    currentElements = with(currentFormula, c(input, productivity, output))
    currentKey = selectedKey
    ## Update the item and element key to for current item
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

    ## NOTE (Michael): The test should be conducted on the raw data, that is
    ##                 excluding any imputation, statistical estimation and
    ##                 previous calculated values. However, auto corrected
    ##                 values will be saved back to the database.
    if(nrow(currentData) > 0){

        ## Correct the flag and values here where available based on agreed
        ## correction rules.
        autoCorrectedData =
            currentData %>%
            fillRecord(data = .) %>%
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


        ## Check flag validity
        errorList[[1]] =
            rawData %>%
            normalise %>%
            ensureFlagValidity(data = .,
                               getInvalidData = TRUE) %>%
            rbind(errorList[[1]], .)

        ## Check production value
        errorList[[2]] =
            with(formulaParameters,
            {
                rawData %>%
                    ensureValueRange(data = .,
                                     ensureColumn = productionValue,
                                     getInvalidData = TRUE) %>%
                    normalise %>%
                    rbind(errorList[[2]], .)
            }
            )

        ## Check area harvested value
        errorList[[3]] =
            with(formulaParameters,
            {
                rawData %>%
                    ensureValueRange(data = .,
                                     ensureColumn = areaHarvestedValue,
                                     getInvalidData = TRUE) %>%
                    normalise %>%
                    rbind(errorList[[3]], .)
            }
            )

        ## Check yield value
        errorList[[4]] =
            with(formulaParameters,
            {
                rawData %>%
                    ensureValueRange(data = .,
                                     ensureColumn = yieldValue,
                                     getInvalidData = TRUE,
                                     includeEndPoint = FALSE) %>%
                    normalise %>%
                    rbind(errorList[[4]], .)
            }
            )

        ## Check production domain balanced.
        errorList[[5]] =
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
                    rbind(errorList[[5]], .)
            }
            )

        ## ## Save the auto-corrected data back
        ## autoCorrectedData %>%
        ##     SaveData(domain = sessionKey@domain,
        ##              dataset = sessionKey@dataset,
        ##              data = .)

    } else {
        message("Current Item has no data")
    }

}


##' Send email to user if the data contains invalid entries.
if(max(sapply(errorList, nrow)) > 0){
    from = "I_am_a_magical_unicorn@sebastian_quotes.com"
    to = swsContext.userEmail
    subject = "Validation Result"
    body = paste0("There are 5 tests current in the system:\n",
                  "1. Flag validity: Whether a flag is valid\n",
                  "2. Production range: [0, Inf)\n",
                  "3. Area Harvested range: [0, Inf)\n",
                  "4. Yield range: (0, Inf)\n",
                  "5. Production balanced")


    ## Flag error
    flagErrorAttachmentName = "flag_error.csv"
    flagErrorAttachmentPath =
        paste0(R_SWS_SHARE_PATH, "/kao/", flagErrorAttachmentName)
    write.csv(errorList[[1]], file = flagErrorAttachmentPath,
              row.names = FALSE)
    flagErrorAttachmentObject = mime_part(x = flagErrorAttachmentPath,
                                          name = flagErrorAttachmentName)

    ## production value error
    prodValueErrorAttachmentName = "prodValue_error.csv"
    prodValueErrorAttachmentPath =
        paste0(R_SWS_SHARE_PATH, "/kao/", prodValueErrorAttachmentName)
    write.csv(errorList[[2]], file = prodValueErrorAttachmentPath,
              row.names = FALSE)
    prodValueErrorAttachmentObject =
        mime_part(x = prodValueErrorAttachmentPath,
                  name = prodValueErrorAttachmentName)


    ## area harvested value error
    areaValueErrorAttachmentName = "areaValue_error.csv"
    areaValueErrorAttachmentPath =
        paste0(R_SWS_SHARE_PATH, "/kao/", areaValueErrorAttachmentName)
    write.csv(errorList[[3]], file = areaValueErrorAttachmentPath,
              row.names = FALSE)
    areaValueErrorAttachmentObject =
        mime_part(x = areaValueErrorAttachmentPath,
                  name = areaValueErrorAttachmentName)


    ## yield value error
    yieldValueErrorAttachmentName = "yieldValue_error.csv"
    yieldValueErrorAttachmentPath =
        paste0(R_SWS_SHARE_PATH, "/kao/", yieldValueErrorAttachmentName)
    write.csv(errorList[[4]], file = yieldValueErrorAttachmentPath,
              row.names = FALSE)
    yieldValueErrorAttachmentObject =
        mime_part(x = yieldValueErrorAttachmentPath,
                  name = yieldValueErrorAttachmentName)


    ## imbalance error
    imbalanceErrorAttachmentName = "imbalance_error.csv"
    imbalanceErrorAttachmentPath =
        paste0(R_SWS_SHARE_PATH, "/kao/", imbalanceErrorAttachmentName)
    write.csv(errorList[[5]], file = imbalanceErrorAttachmentPath,
              row.names = FALSE)
    imbalanceErrorAttachmentObject =
        mime_part(x = imbalanceErrorAttachmentPath,
                  name = imbalanceErrorAttachmentName)

    ## Send the mail
    bodyWithAttachment =
        list(body,
             flagErrorAttachmentObject,
             prodValueErrorAttachmentObject,
             areaValueErrorAttachmentObject,
             yieldValueErrorAttachmentObject,
             imbalanceErrorAttachmentObject)
    sendmail(from = from, to = to, subject = subject, msg = bodyWithAttachment)
    stop("Production Input Invalid, please check follow up email on invalid data")
} else {
    msg = "Production Input Validation passed without any error!"
    message(msg)
}


