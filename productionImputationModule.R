## load the library
library(faosws)
library(faoswsUtil)
library(faoswsFlag)
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
    apiDirectory = "~/Documents/Github/faoswsProduction/R/"

    ## Get SWS Parameters
    GetTestEnvironment(
        baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
        token = "22fb397d-d9e6-416d-acb8-717451714c62"
        ## baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
        ## token = "90bb0f92-e345-4401-945d-1e43af801167"
    )

    ## Source local scripts for this local test
    for(file in dir(apiDirectory, full.names = T))
        source(file)
}

fullKey = swsContext.datasets[[1]]
subKey = fullKey
uniqueItem = fullKey@dimensions$measuredItemCPC@keys
for(singleItem in uniqueItem){
    subKey@dimensions$measuredItemCPC@keys = singleItem
    print(paste0("Imputation for item: ", singleItem))
    
    impute = try({
        cat("Reading in the data...\n")
        datasets = getProductionData(subKey)
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
            processingParams = 
            computeYield(datasets$query, newMethodFlag = "i",
                         processingParameters = processingParams)

            ## Impute yield
            yieldCode = datasets$formulaTuples[, productivity][i]
            yieldParams = 
                defaultImputationParameters(variable = as.numeric(yieldCode))
            ## Change the model formula to use a hierarchical mixed model.  The
            ## code to do this is a bit messy because we have to adjust the
            ## default argument of the model function (which is an element of
            ## the S4 ensembleModel object)
            formals(yieldParams$ensembleModels$defaultMixedModel@model)$modelFormula =
                paste0(yieldValue, " ~ -1 + (1 + bs(timePointYears,",
                       "df = 2, degree = 1)|geographicAreaM49)")
            yieldParams$estimateNoData = TRUE
            ## Impute production
            productionCode = datasets$formulaTuples[, input][i]
            productionParams = 
                defaultImputationParameters(variable = as.numeric(productionCode))
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