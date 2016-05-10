<<<<<<< HEAD
##' Check whether the imputation model exist
##'
##' @param modelLoadingPath The path to where imputed datasets are placed.
##' @param item The commodity item code
##'
##' @return logical, whether the specific model exist
##'
##' @export

imputationExist = function(modelLoadingPath, item){
    modelName = createImputationObjectName(item = item)
    file.exists(paste0(modelLoadingPath, modelName))
}
