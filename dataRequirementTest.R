##' This test is designed to explore whether all the data in the
##' production domain met the data requirement.
##'
##' All values within the selected sets are imputed
##
##' Definition of the set:
##'
##'     1. Required list of country, item, element and years provided
##'        by the user.
##'
##'        NOTE (Michael): This list is currently obtained from the
##'                        getMainKey() function, this should however
##'                        be changed to read from the requirement
##'                        database.
##'
##'     2. The record need to exist in the system.
##'


library(magrittr)
library(faosws)
library(faoswsProduction)

## Setting up variables
areaVar = "geographicAreaM49"
yearVar = "timePointYears"
itemVar = "measuredItemCPC"
elementVar = "measuredElement"
server = "Prod"

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
DEBUG_MODE = Sys.getenv("R_DEBUG_MODE")

if(!exists("DEBUG_MODE") || DEBUG_MODE == ""){
    cat("Not on server, so setting up environment...\n")
    
    stopifnot(server %in% c("QA", "Prod"))
    ## Define directories
    if(Sys.info()[7] == "josh"){
        apiDirectory = "~/Documents/Github/faoswsProduction/R/"
        R_SWS_SHARE_PATH = ifelse(server == "Prod", "/media/hqlprsws2_prod/",
                                  "/media/hqlprsws1_qa/")
        SetClientFiles(dir = ifelse(server == "Prod",
                                    "~/R certificate files/Production/",
                                    "~/R certificate files/QA/"))
        # runParallel = TRUE
    } else if(Sys.info()[7] == "rockc_000"){
        apiDirectory = "~/Github/faoswsProduction/R/"
        stop("Can't connect to share drives!")
    } else if(Sys.info()[7] == "mk"){
        apiDirectory = "R/"
        R_SWS_SHARE_PATH =
            ifelse(server == "Prod",
                   "/media/sws_prod_shared_drive/",
                   "/media/sws_qa_shared_drive/")
        SetClientFiles(dir = ifelse(server == "Prod", "~/.R/prod", "~/.R/qa"))
    }

    ## Get SWS Parameters

    if(server == "Prod"){
        GetTestEnvironment(
            baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
            token = "ad797167-20aa-4ff4-b679-461c96e0da79"
        )
    } else {
        GetTestEnvironment(
            baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
            token = "5eed31f5-2318-470e-a884-c30c3db6d3db"
        )
    }
}

normaliseFormula = function(formula){
    normalisedFormula =
        melt(formula[, c("measuredItemCPC", "input", "productivity", "output"),
                          with = FALSE],
             id.vars = "measuredItemCPC")
    normalisedFormula[, variable := NULL]
    setnames(normalisedFormula, "value", "measuredElement")
    return(normalisedFormula)
}

getCompleteData = function(key){
    productionData = GetData(key)
    formula = getYieldFormula(key@dimensions$measuredItemCPC@keys) %>%
        normaliseFormula(.)
    merge(productionData, formula, by = colnames(formula),
          all = TRUE, allow.cartesian = TRUE)
}

convert0MtoNA = function(data, valueVars, flagVars, missingFlag = "M"){
    dataCopy = copy(data)
    mapply(FUN = function(value, flag){
        dataCopy[which(dataCopy[[flag]] == missingFlag), `:=`(c(value), NA_real_)]
    }, value = valueVars, flag = flagVars)
    dataCopy
}

getUnimputedTimeSeries = function(data, key, valueColumn){
    ## The number of missing values should be either zero or all
    ## missing.
    check = data[, sum(is.na(.SD[[valueColumn]])) == 0 |
                   sum(is.na(.SD[[valueColumn]])) == .N,
                 by = c(key)]
    originalData = copy(data)
    setkeyv(check, key)
    setkeyv(originalData, key)

    ## Get the unimputed data set
    unimputedKeys = check[!check$V1, key, with = FALSE]
    unimputedData = originalData[unimputedKeys, ]
    unimputedData
}

## Test whether the all the data are imputed
years = 1991:year(Sys.Date())
unimputedTimeSeries =
    years %>%
    getMainKey(.) %>%
    getCompleteData(.) %>%
    convert0MtoNA(data = ., valueVars = "Value",
                  flagVars = "flagObservationStatus") %>%
    getUnimputedTimeSeries(data = .,
                           key = c("geographicAreaM49",
                                   "measuredItemCPC",
                                   "measuredElement"),
                           valueColumn = "Value")

## Check whether all the animal numbers in animals (parent) and animal
## slaughtered in meat (child) are synchronised.
##
## NOTE (Michael): This function doesn't work at the moment as it is
##                 sitting on the "module_test" branch.
##
## checkSlaughteredSynced()

if(NROW(unimputedTimeSeries) > 0){
    stop("Not all time series are imputed")
} else {
    message("Data requirement is satisfied")
}
    
