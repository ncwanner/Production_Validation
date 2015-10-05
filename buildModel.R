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
years = 1997:2013

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
DEBUG_MODE = Sys.getenv("R_DEBUG_MODE")

if(!exists("DEBUG_MODE") || DEBUG_MODE == ""){
    cat("Not on server, so setting up environment...\n")
    
    ## Define directories
    if(Sys.info()[7] == "josh"){
        apiDirectory = "~/Documents/Github/faoswsProduction/R/"
        R_SWS_SHARE_PATH = "~/Documents/Github/faoswsProduction/Model/"
        ## R_SWS_SHARE_PATH = "/media/hqlprsws2_prod"
    } else if(Sys.info()[7] == "rockc_000"){
        apiDirectory = "~/Github/faoswsProduction/R/"
        stop("Can't connect to share drives!")
    }

    ## Get SWS Parameters
    SetClientFiles(dir = "~/R certificate files/QA")
    GetTestEnvironment(
        ## baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
        ## token = "7b588793-8c9a-4732-b967-b941b396ce4d"
        baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
        token = "3242f11d-28b2-4429-86b0-6fab97cb50bb"
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
                                   keys = as.character(years)) # 15 years
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
                       " ~ -1 + (1 + bs(", yearVar, ", df = 2, degree = 1)",
                       "|", areaVar, ")")
            yieldParams$estimateNoData = TRUE
            yieldParams$byKey = c(areaVar, itemVar)
            ## Impute production
            productionCode = datasets$formulaTuples[, input][i]
            productionParams = 
                defaultImputationParameters(variable = as.numeric(productionCode))
            warning("There's a missing '_' in the faoswsImputation package.  ",
                    "Once that's fixed, the line below won't be needed.")
            productionParams$imputationMethodColumn = paste0(
                "flagMethod_measuredElement_", as.numeric(productionCode))
            productionParams$estimateNoData = TRUE
            productionParams$byKey = c(areaVar, itemVar)
            
            datasets$query[, countryCnt := .N, by = c(processingParams$byKey)]
            datasets$query = datasets$query[countryCnt > 1, ]
            datasets$query[, countryCnt := NULL]
            modelYield = faoswsImputation:::buildEnsembleModel(
                data = datasets$query, imputationParameters = yieldParams,
                processingParameters = processingParams)
            yieldVar = paste0("Value_measuredElement_", yieldCode)
            ## Have to save the yield estimates to the data because we need to 
            ## balance and then impute production.  Also, check if fit is NULL
            ## because it throws an error if no observations are missing and
            ## estimated.
            if(!is.null(modelYield$fit$fit)){
                datasets$query[is.na(get(yieldVar)), yieldVar := modelYield$fit$fit]
            }
            
            ## Impute production
            balanceProduction(data = datasets$query,
                              processingParameters = processingParams)
            modelProduction = faoswsImputation:::buildEnsembleModel(
                data = datasets$query, imputationParameters = productionParams,
                processingParameters = processingParams)
            
            ## Save models
            save(modelYield, modelProduction,
                 file = paste0(R_SWS_SHARE_PATH, "prodModel_", singleItem,
                               "_", i, ".RData"))
        } # close item type for loop
    }) # close try block
    if(inherits(impute, "try-error")){
        print("Imputation Module Failed")
    } else {
        print("Imputation Module Executed Successfully")
    }
}

"Module completed!"