## load the library
library(faosws)
library(faoswsUtil)
library(faoswsFlag)
library(faoswsImputation)
library(data.table)
library(splines)
library(lme4)

## Setting up variables
areaVar = "geographicAreaM49"
yearVar = "timePointYears"
itemVar = "measuredItemCPC"
elementVar = "measuredElement"

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
DEBUG_MODE = Sys.getenv("R_DEBUG_MODE")

if(!exists("DEBUG_MODE") || DEBUG_MODE == ""){
    cat("Not on server, so setting up environment...\n")
    
    ## Define directories
    if(Sys.info()[7] == "josh")
        apiDirectory = "~/Documents/Github/faoswsProduction/R/"
    if(Sys.info()[7] == "rockc_000")
        apiDirectory = "~/Github/faoswsProduction/R/"

    ## Get SWS Parameters
    GetTestEnvironment(
        ## baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
        ## token = "e77abee7-9b0d-4557-8c6f-8968872ba7ca"
        baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
        token = "c585c410-fb9e-44ea-ba36-ef940d32185d"
    )

    ## Source local scripts for this local test
    for(file in dir(apiDirectory, full.names = T))
        source(file)
}

allCountryCodes = GetCodeList(domain = slot(swsContext.datasets[[1]], "domain"),
                              dataset = slot(swsContext.datasets[[1]], "dataset"),
                              dimension = areaVar)
allCountryCodes = unique(allCountryCodes[type == "country", code])

allItemCodes = GetCodeList(domain = slot(swsContext.datasets[[1]], "domain"),
                           dataset = slot(swsContext.datasets[[1]], "dataset"),
                           dimension = "measuredItemCPC")
warning("Check that these are the right items!!!")
allItemCodes = unique(allItemCodes[!is.na(type), code])

selectedYears =
    slot(slot(swsContext.datasets[[1]], "dimensions")$timePointYears,
         "keys")

yieldFormula = GetTableData(schemaName = "ess",
                            tableName = "item_type_yield_elements")
productionElements = unique(unlist(yieldFormula[, list(element_31, element_41,
                                                       element_51)]))

fullKey = DatasetKey(
    domain = slot(swsContext.datasets[[1]], "domain"),
    dataset = slot(swsContext.datasets[[1]], "dataset"),
    dimensions = list(
        geographicAreaM49 = Dimension(name = "geographicAreaM49",
                                      keys = allCountryCodes),
        measuredElement = Dimension(name = "measuredElement",
                                    keys = productionElements),
        measuredItemCPC = Dimension(name = "measuredItemCPC",
                                    keys = allItemCodes),
        timePointYears = Dimension(name = "timePointYears",
                                   keys = as.character(1997:2011)) # 15 years
        )
    )

subKey = fullKey
uniqueItem = fullKey@dimensions$measuredItemCPC@keys
for(singleItem in uniqueItem){
    subKey@dimensions$measuredItemCPC@keys = singleItem
    print(paste0("Imputation for item: ", singleItem))
    
    impute = try({
        cat("Reading in the data...\n")
        datasets = getProductionData(dataContext = subKey)
        ## NOTE (Michael): The yield should have been
        ##                 calculated a priori to the
        ##                 imputation module.
        
        ## Some commodities have multiple formulas.  For example, the LNSP
        ## (livestock non-primary) item type has beef, indigenous beef, and
        ## biological beef.  Rather than being different commodities, these
        ## three commodities are stored under the beef commodity with
        ## different element codes for production/yield/output.  So, we
        ## need to process each one and thus loop over all the
        ## formulaTuples (which specifies the multiple element codes if
        ## applicable).
        for(i in 1:nrow(datasets$formulaTuples)){
            cat("Processing pair", i, "of", nrow(datasets$formulaTuples),
                "element triples.\n")
            
#             ## Set the names
#             assignColumnNameVars(prefixTuples = datasets$prefixTuples,
#                                  formulaTuples = datasets$formulaTuples[i])

            ## Recompute the yield
            cat("Computing yield...\n")
            processingParams = defaultProcessingParameters(
                productionValue = datasets$formulaTuples[, output][i],
                yieldValue = datasets$formulaTuples[, productivity][i],
                areaHarvestedValue = datasets$formulaTuples[, input][i])
            computeYield(datasets$query, newMethodFlag = "i",
                         processingParameters = processingParams)

            ## Impute yield
            yieldCode = datasets$formulaTuples[, productivity][i]
            yieldParams = 
                defaultImputationParameters(variable = as.numeric(yieldCode))
            warning("There's a missing '_' in the faoswsImputation package.  ",
                    "Once that's fixed, the line below won't be needed.")
            yieldParams$imputationMethodColumn = paste0(
                "flagMethod_measuredElement_", as.numeric(yieldCode))
            ## Change the model formula to use a hierarchical mixed model.  The
            ## code to do this is a bit messy because we have to adjust the
            ## default argument of the model function (which is an element of
            ## the S4 ensembleModel object)
            formals(yieldParams$ensembleModels$defaultMixedModel@model)$modelFormula =
                paste0(yieldParams$imputationValueColumn,
                       " ~ -1 + (1 + bs(timePointYears, df = 2, degree = 1)",
                       "|geographicAreaM49)")
            yieldParams$estimateNoData = TRUE
            ## Impute production
            productionCode = datasets$formulaTuples[, input][i]
            productionParams = 
                defaultImputationParameters(variable = as.numeric(productionCode))
            warning("There's a missing '_' in the faoswsImputation package.  ",
                    "Once that's fixed, the line below won't be needed.")
            productionParams$imputationMethodColumn = paste0(
                "flagMethod_measuredElement_", as.numeric(productionCode))
            productionParams$estimateNoData = TRUE
            imputed = imputeProductionDomain(data = datasets$query,
                                             processingParameters = processingParams,
                                             yieldImputationParameters = yieldParams,
                                             productionImputationParameters = productionParams)

            ## Save back to database
            saveProductionData(imputed,
                    areaHarvestedCode = datasets$formulaTuples$input[i],
                    yieldCode = datasets$formulaTuples$productivity[i],
                    productionCode = datasets$formulaTuples$output[i],
                    verbose = TRUE)
        } # close item type for loop
    }) # close try block
    if(inherits(impute, "try-error")){
        print("Imputation Module Failed")
    } else {
        print("Imputation Module Executed Successfully")
    }
}

"Module completed!"