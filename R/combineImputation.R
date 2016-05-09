##' This function combines imputation from both model in which one can contain
##' estimated figures while the other does not.
##'
##' @param imputationWithEstimates The imputed dataset where estimates are
##'     included
##' @param imputationWithoutEstimates The imputed dataset where estimates were
##'     removed.
##'
##' @return A combined dataset where rbind of both dataset
##'
##' @export
##'

combineImputation = function(imputationWithEstimates,
                             imputationWithoutEstimates){
    combinedImputation = rbind(imputationWithoutEstimates,
                               imputationWithEstimates)
    combinedImputation[, useEstimates := NULL]
    combinedImputation
}
