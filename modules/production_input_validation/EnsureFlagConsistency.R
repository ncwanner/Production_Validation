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


##' Loop through the formulas
for(i in seq(nrow(unique_formulas))){
    ## Subset the formula table
    current_formula = unique_formulas[10, ]
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
    data =
        GetData(subKey) 
    
    data=denormalise(data, denormaliseKey = "measuredElement")
    
    data[, flagCombOutput:=paste(get(formulaParameters$productionObservationFlag),
                               get(formulaParameters$productionMethodFlag),sep=";")]
    
    data[, flagCombInput:=paste(get(formulaParameters$areaHarvestedObservationFlag),
                               get(formulaParameters$areaHarvestedMethodFlag),sep=";")]
    
    AtLeastOne=data[flagCombOutput=="M;-" | flagCombInput=="M;-",]
    AtLeastOne[,doubleComb:=paste(flagCombOutput,flagCombInput,sep=";")]
    
    AtLeastOne=AtLeastOne[doubleComb!="M;-;M;-",]
    AtLeastOne=AtLeastOne[,doubleComb:=NULL]
    
    flagProtected=flagValidTable[Protected==TRUE,]
    flagProtected[,comb:=paste(flagObservationStatus,flagMethod,sep=";")]
    
    ## First situation: Input=M- Output is non protected
    
    AtLeastOne[flagCombInput=="M;-" & !flagCombOutput %in% flagProtected[,comb], formulaParameters$productionObservationFlag:="M"]
    AtLeastOne[flagCombInput=="M;-" & !flagCombOutput %in% flagProtected[,comb], formulaParameters$productionMethodFlag:="-"]
    
    
    ## second situation: Output=M- and Input is in non protected
    
    AtLeastOne[flagCombOutput=="M;-" & !flagCombInput %in% flagProtected[,comb], formulaParameters$areaHarvestedObservationFlag:="M"]
    AtLeastOne[flagCombOutput=="M;-" & !flagCombInput %in% flagProtected[,comb], formulaParameters$areaHarvestedMethodFlag:="-"]
    
    ## third situation Input=M- Output is PROTECTED
    
    AtLeastOne[flagCombInput=="M;-" & flagCombOutput %in% flagProtected[,comb],
               formulaParameters$areaHarvestedObservationFlag:="M"]
    
    
    AtLeastOne[flagCombInput=="M;-" & flagCombOutput %in% flagProtected[,comb], 
               formulaParameters$areaHarvestedMethodFlag:="u"]
    
    ## fourth situation Output=M- Output is PROTECTED
    
    AtLeastOne[flagCombOutput=="M;-" & flagCombInput %in% flagProtected[,comb], formulaParameters$productionObservationFlag:="M"]
    AtLeastOne[flagCombOutput=="M;-" & flagCombInput %in% flagProtected[,comb], formulaParameters$productionMethodFlag:="u"]
    

    
    
    
    AtLeastOne[,flagCombOutput:=NULL]
    AtLeastOne[,flagCombInput:=NULL]
    
    AtLeastOne %>%
        normalise(.) %>%
        postProcessing(.) %>%
        SaveData(domain = sessionKey@domain,
                 dataset = sessionKey@dataset,
                 data = .)
    
    
    
}
