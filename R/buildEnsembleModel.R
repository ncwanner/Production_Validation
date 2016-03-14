##' Build Ensemble Model
##' 
##' This function should build an R object which contains all the information 
##' from the ensemble models.  Thus, the object should be able to be loaded and 
##' used to fill in imputed values and generate predictions using the originally
##' developed model (rather than requiring the model be refit each time).
##' 
##' To actually construct and save such a model object is unfortunately quite a 
##' large task: we must save a model object for each individual model in 
##' addition to the weights/errors from fitting these models to the data.  Thus,
##' instead of this task, we can just save the imputations and fill those in 
##' when the impute module is called.  This also gives us the ability to version
##' control the imputations: we could put them in a new table in the SWS.
##' 
##' @param data The data.table object containing the dataset we wish to impute 
##'   on.
##' @param imputationParameters A list of the parameters for the yield 
##'   imputation.  See defaultImputationParameters() for a starting point.
##' @param processingParameters A list of the parameters for the production 
##'   processing algorithms.  See defaultProductionParameters() for a starting 
##'   point.
##' @param unitConversion A ratio specifying the relationship between yield, 
##'   production, and area harvested.
##'   
##' @return A list containing the model fit, model errors, and model weights.
##'   

buildEnsembleModel = function(data, imputationParameters, processingParameters,
                              unitConversion){
    
    ## Data Quality Checks
    if(!exists("ensuredImputationData") || !ensuredImputationData)
        ensureImputationInputs(data = data,
                               imputationParameters = imputationParameters)

    ## Pre-process the data
    setkeyv(x = data, cols = c(processingParameters$byKey,
                               processingParameters$yearValue))
    data = processProductionDomain(data = data,
                            processingParameters = processingParameters)
    computeYield(data, processingParameters = processingParameters,
                 unitConversion = unitConversion)
    valueMissingIndex = is.na(
        data[[imputationParameters$imputationValueColumn]])
    flagMissingIndex = (data[[imputationParameters$imputationFlagColumn]] ==
                            imputationParameters$missingFlag)

    # Ensure missing values agree with missing flags
    if(!all(valueMissingIndex == flagMissingIndex)){
        cat("Values that are NA: ", sum(valueMissingIndex), "\n")
        cat("Flags with missingFlag value: ", sum(flagMissingIndex), "\n")
        stop("Different missing values from flags/values!  Maybe call remove0M?")
    }
    if(is.null(names(imputationParameters$ensembleModels)))
        names(imputationParameters$ensembleModels) = paste(
            "Model", 1:length(imputationParameters$ensembleModels), sep = "_")
    if(!any(is.na(data[[imputationParameters$imputationValueColumn]]))){
        warning("No missing values in data[[imputationValueColumn]].",
        "Returning empty data.tables")
        fit = data.table(fit = 0, variance = 0, timePointYears = 0,
                         geographicAreaM49 = "0", measuredItemCPC = "0")
        fit = fit[-1, ]
        errors = data.table(geographicAreaM49 = "0")
        errors[, names(imputationParameters$ensembleModels) := 0]
        errors = errors[-1, ]
        return(list(fit = fit, errors = errors))
    }
    if(min(data[, .N, by = c(processingParameters$byKey)]$N) == 1){
        print(data[, .N, by = c(processingParameters$byKey)][N == 1, ])
        stop("Only one data point in time series for above data!")
    }

    ## Define which columns should store the imputation and flags, and create
    ## those columns if they don't currently exist.
    if(imputationParameters$newImputationColumn == ""){
        newValueColumn = imputationParameters$imputationValueColumn
        newObsFlagColumn = imputationParameters$imputationFlagColumn
        newMethodFlagColumn = imputationParameters$imputationMethodColumn
    } else {
        newValueColumn = paste0("Value_",
                                imputationParameters$newImputationColumn)
        newObsFlagColumn = paste0("flagObservationStatus_",
                                imputationParameters$newImputationColumn)
        newMethodFlagColumn = paste0("flagMethod_",
                                imputationParameters$newImputationColumn)
    }
    newVarianceColumn = "ensembleVariance"

    ## Order data by byKey and then by year
    setkeyv(x = data, cols = c(imputationParameters$byKey,
                               imputationParameters$yearValue))
    
    ## Build the ensemble
    ensemble = data[[imputationParameters$imputationValueColumn]]
    missIndex = is.na(ensemble)
    cvGroup = makeCvGroup(data = data,
                          imputationParameters = imputationParameters)
    modelFits = computeEnsembleFit(data = data,
                                   imputationParameters = imputationParameters)
    modelStats = computeEnsembleWeight(data = data,
        cvGroup = cvGroup, fits = modelFits,
        imputationParameters = imputationParameters)
    modelWeights = modelStats[[1]]
    modelErrors = modelStats[[2]]
    if(imputationParameters$plotImputation != ""){
        plotEnsemble(data = data, modelFits = modelFits,
                     modelWeights = modelWeights, ensemble = ensemble,
                     imputationParameters = imputationParameters,
                     returnFormat = imputationParameters$plotImputation)
#         plotEnsembleOld(data, modelFits, modelWeights, ensemble)
    }

    ## Compute objects to save
    finalFit = computeEnsemble(fits = modelFits, weights = modelWeights,
                               errors = modelErrors)
    finalFit[, c(imputationParameters$yearValue) :=
                 data[[imputationParameters$yearValue]]]
    for(name in imputationParameters$byKey){
        finalFit[, c(name) := data[[name]]]
    }
    for(name in imputationParameters$byKey){
        modelErrors[, c(name) := data[[name]]]
    }
    modelErrors = reshape2::melt(modelErrors,
                                 id.vars = imputationParameters$byKey)
    ## Use max to pull out the non-zero errors.  All error entries will be
    ## either 0 or the error from the ensemble, so we just need max.
    modelErrors = modelErrors[, max(value),
                              by = c("variable", imputationParameters$byKey)]
    modelErrors = dcast.data.table(modelErrors,
                                   formula = paste0(imputationParameters$byKey,
                                                    "~ variable"),
                                   value.var = "V1", fun.aggregate = mean)

    return(list(fit = finalFit[missIndex, ], errors = modelErrors))
}