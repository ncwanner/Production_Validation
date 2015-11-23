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
years = 1991:2014

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
DEBUG_MODE = Sys.getenv("R_DEBUG_MODE")

if(!exists("DEBUG_MODE") || DEBUG_MODE == ""){
    cat("Not on server, so setting up environment...\n")
    
    ## Define directories
    if(Sys.info()[7] == "josh"){
        apiDirectory = "~/Documents/Github/faoswsProduction/R/"
        R_SWS_SHARE_PATH = "/media/hqlprsws1_qa/"
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
        token = "9999fcbd-b439-401e-a572-717908f0b0dc"
    )

    ## Source local scripts for this local test
    for(file in dir(apiDirectory, full.names = T))
        source(file)
}

allCountryCodes = GetCodeList(domain = slot(swsContext.datasets[[1]], "domain"),
                              dataset = slot(swsContext.datasets[[1]], "dataset"),
                              dimension = areaVar)
allCountryCodes = unique(allCountryCodes[type == "country", code])
## HACK: Update China
warning("Hack below!  Fix once the geographicAreaM49 dimension is fixed!")
allCountryCodes = allCountryCodes[!allCountryCodes %in% c("1249", "156")]
allCountryCodes = c(allCountryCodes, "158", "1248")
allCountryCodes = unique(allCountryCodes)

allItemCodes = GetCodeList(domain = slot(swsContext.datasets[[1]], "domain"),
                           dataset = slot(swsContext.datasets[[1]], "dataset"),
                           dimension = "measuredItemCPC")
allItemCodes = unique(allItemCodes[!is.na(type), code])
allPrimaryCodes = c("01921.01", "02111", "02112", "02131", "02122", "02123",
                 "02191", "02140", "02132", "02151", "02154", "02153", "02152",
                 "01270", "01253.02", "0142", "01371", "01374", "01376",
                 "01323", "01322", "0141", "01445", "01443", "01447", "01441",
                 "01449.90", "02291", "02292", "01199.90", "0117", "01705",
                 "01312", "01801", "01640", "01610", "0231", "01460", "02194",
                 "01290.90", "01359.90", "01442", "01444", "02211", "0111",
                 "0112", "0118", "0113", "01703", "01707", "01809", "0115",
                 "01491.01", "0114", "01520.01", "01802", "01950.01", "01510",
                 "02121.01", "01234", "01343", "01315", "01321", "01342.01",
                 "01345", "01446", "0116", "01314", "01599.91", "01313",
                 "21170.92", "01318", "01192", "01195", "01199.02", "01704",
                 "01330", "01379.90", "01231", "01372", "01316", "01709.90",
                 "01346", "01450", "01341", "01311", "01290.01", "01241.02",
                 "01232", "01324", "01691", "01344.01", "01329", "01319",
                 "21170.02", "01212", "01213", "01216", "01233",  "01235",
                 "01215", "01449.02", "01344.02", "01351.02", "01499.01",
                 "01193", "01242", "01550", "01701", "01251",  "01530",
                 "01253.01", "01449.01", "01499.02", "01375", "01709.01",
                 "02910", "02212", "01499.05", "01342.02", "01354", "01930.02",
                 "01243", "01252", "01349.20", "01211", "01351.01", "01353.01",
                 "02121.02", "02192.01", "01191", "01377", "02133", "01355.90")
allDerivedCodes = c(306, 307, 1242, 162, 165, 237, 244, 252, 256, 257, 258,
                    261, 268, 271, 281, 290, 329, 331, 334, 51, 564, 60, 767,
                    1745, 1809, 1811, 1816, 1021, 1022, 1043, 1186, 1225, 885,
                    886, 887, 888, 889, 890, 891, 894, 895, 896, 897, 898, 899,
                    900, 901, 904, 952, 953, 955, 983, 984)
allDerivedCodes = faoswsUtil::fcl2cpc(formatC(allDerivedCodes, width = 4, flag = "0"))
allItemCodes = c(allPrimaryCodes, allDerivedCodes)
allItemCodes = allItemCodes[!is.na(allItemCodes)]

warning("Should identify this with faoswsUtil:::getCommodityTree")

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

successCount = 0
failCount = 0

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

            valueCols = paste0(datasets$prefixTuples$valuePrefix,
                               datasets$formulaTuples[, c("productivity", "output"),
                                                      with = FALSE])
            if(all(is.na(datasets$query[[valueCols[1]]])) &
               all(is.na(datasets$query[[valueCols[2]]]))){
                stop("No non-missing data!")
                next
            }
            
            ## Some rows may be missing entirely, and thus we may fail to impute
            ## for those years/countries/commodities if we don't add rows with
            ## missing data.  Do that here:
            countryCommodity = unique(datasets$query[, c(areaVar, itemVar),
                                                     with = FALSE])
            ## To merge two data.tables, we need a key column.  Create a dummy
            ## one to do the merge.
            countryCommodity[, mergeKey := 1]
            year = data.table(years)
            setnames(year, yearVar)
            year[, mergeKey := 1]
            fullData = merge(countryCommodity, year, by = "mergeKey",
                             allow.cartesian = TRUE)
            fullData[, mergeKey := NULL]
            ## Merge fullData back to datasets$query.  If a record was missing
            ## in datasets$query, it will now exist with NA values/flags.
            datasets$query = merge(datasets$query, fullData,
                                   by = c(itemVar, areaVar, yearVar),
                                   all.y = TRUE)
            ## Replace NA/NA/NA with 0/M/n
            valCols = grep("Value_", colnames(datasets$query), value = TRUE)
            obsFlagCols = grep("flagObservation", colnames(datasets$query),
                               value = TRUE)
            metFlagCols = grep("flagMethod", colnames(datasets$query),
                               value = TRUE)
            sapply(valCols, function(colname){
                datasets$query[is.na(get(colname)), c(colname) := 0]})
            sapply(obsFlagCols, function(colname){
                datasets$query[is.na(get(colname)), c(colname) := "M"]})
            sapply(metFlagCols, function(colname){
                datasets$query[is.na(get(colname)), c(colname) := "n"]})

            ## Recompute the yield
            cat("Computing yield...\n")
            processingParams = defaultProcessingParameters(
                productionValue = datasets$formulaTuples[, output][i],
                yieldValue = datasets$formulaTuples[, productivity][i],
                areaHarvestedValue = datasets$formulaTuples[, input][i])
            ## Don't remove old estimated imputation values, per request from
            ## Salar.
            processingParams$removePriorImputation = FALSE
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
                paste0(yieldParams$imputationValueColumn,
                       " ~ -1 + (1 + bs(", yearVar, ", df = 2, degree = 1)",
                       "|", areaVar, ")")
            yieldParams$estimateNoData = TRUE
            yieldParams$byKey = c(areaVar, itemVar)
            ## Impute production
            productionCode = datasets$formulaTuples[, output][i]
            productionParams = 
                defaultImputationParameters(variable = as.numeric(productionCode))
            productionParams$estimateNoData = TRUE
            productionParams$byKey = c(areaVar, itemVar)
            
            datasets$query[, countryCnt := .N, by = c(processingParams$byKey)]
            datasets$query = datasets$query[countryCnt > 1, ]
            datasets$query[, countryCnt := NULL]
            png(paste0(R_SWS_SHARE_PATH, "/browningj/production/imputationPlots/yield_",
                               singleItem, "_", i, ".png"), width = 2300, height = 2300)
            modelYield = try(faoswsImputation:::buildEnsembleModel(
                data = datasets$query, imputationParameters = yieldParams,
                processingParameters = processingParams))
            dev.off()
            if(is(modelYield, "numeric")){
                ## modelYield **should** be a list, but if no data is missing a 
                ## vector is returned.  In this case, just balance and continue.
                balanceProduction(data = datasets$query,
                                  processingParameters = processingParams)
                modelYield = NULL
            } else if(!is(modelYield, "try-error")){
                ## In this case, we have the expected behaviour: modelYield is a
                ## list.
                yieldVar = paste0("Value_measuredElement_", yieldCode)
                ## Yield must be zero if production or area harvested are 0.
                zeroYield = datasets$query[get(processingParams$areaHarvestedValue) == 0 |
                                           get(processingParams$productionValue) == 0,
                                           c(yieldParams$yearValue, yieldParams$byKey),
                                           with = FALSE]
                datasets$query =
                    datasets$query[!zeroYield, on = c(yieldParams$yearValue, yieldParams$byKey)]
                ## Have to save the yield estimates to the data because we need
                ## to balance and then impute production.  Also, check if fit is
                ## NULL because it throws an error if no observations are
                ## missing and estimated.
                if(!is.null(modelYield$fit$fit)){
                    datasets$query[is.na(get(yieldVar)),
                                   yieldVar := modelYield$fit$fit]
                }
                balanceProduction(data = datasets$query,
                                  processingParameters = processingParams)
            } else {
                ## If model building failed, we still want to continue in case
                ## we can build a production model.
                modelYield = NULL
            }
            
            ## Impute production
            png(paste0(R_SWS_SHARE_PATH, "/browningj/production/imputationPlots/production_",
                   singleItem, "_", i, ".png"), width = 2300, height = 2300)
            modelProduction = faoswsImputation:::buildEnsembleModel(
                data = datasets$query, imputationParameters = productionParams,
                processingParameters = processingParams)
            dev.off()
            
            ## Save models
            save(modelYield, modelProduction, years,
                 file = paste0(R_SWS_SHARE_PATH, "/browningj/production/prodModel_",
                               singleItem, "_", i, ".RData"))
        } # close item type for loop
    }) # close try block
    if(inherits(impute, "try-error")){
        print("Imputation Module Failed")
        failCount = failCount + 1
    } else {
        print("Imputation Module Executed Successfully")
        successCount = successCount + 1
    }
}

paste0("Successfully built ", successCount, " models out of ",
       failCount + successCount, " commodities.")

# builtModels = dir(paste0(R_SWS_SHARE_PATH, "/browningj/production/"),
#                   pattern = "prodModel*")
# builtModels = stringr::str_extract(builtModels, "[0-9.]+")
# builtModels = unique(builtModels)
# missingModels = allItemCodes[!allItemCodes %in% builtModels]