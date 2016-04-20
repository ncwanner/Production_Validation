##' Get Imputation Parameters
##' 
##' This function contains the logic that we'll use to define the imputation 
##' models and their parameters.
##' 
##' @param datasets The object being imputed on, typically created by 
##'   getProductionData.
##' @param i The row number of formulaTuples.  This allows R to produce the
##'   correct formula tuple/yield codes, and is typically 1.
##'   
##' @return A list with the yield and production imputation parameters.
##'   

getImputationParameters = function(datasets, i = 1,
                                   areaVar = "geographicAreaM49",
                                   itemVar = "measuredItemCPC",
                                   elementVar = "measuredElement",
                                   yearVar = "timePointYears"){

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
    ## Add moving average model with period of 3 years
    yieldParams$ensembleModels[[length(yieldParams$ensembleModels)+1]] =
        ensembleModel(model = defaultMovingAverage,
                      extrapolationRange = Inf, level = "local")
    names(yieldParams$ensembleModels)[[length(yieldParams$ensembleModels)]] =
        "defaultMovingAverage"
    ## Impute production
    productionCode = datasets$formulaTuples[, output][i]
    productionParams = 
        defaultImputationParameters(variable = as.numeric(productionCode))
    productionParams$estimateNoData = TRUE
    productionParams$byKey = c(areaVar, itemVar)
    ## Add moving average model with period of 3 years
    productionParams$ensembleModels[[length(productionParams$ensembleModels)+1]] =
        ensembleModel(model = defaultMovingAverage,
                      extrapolationRange = Inf, level = "local")
    names(productionParams$ensembleModels)[[length(productionParams$ensembleModels)]] =
        "defaultMovingAverage"
    return(list(yieldParams = yieldParams, productionParams = productionParams))
}
