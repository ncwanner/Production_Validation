##' This function determine which observations shouldn't be imputed:
##' Production should not be imputed on 0Mn observations.  These are
##' "missing but assumed negligble."  Additionally, we have not been
##' able to identify 0M from the old system as 0Mu or 0Mn and have
##' thus assigned them the flags 0M-.  These should be treated as 0Mn
##' in this case.
##'
##' @param data The data.table object containing the data.
##' @param processingParams A list of the parameters for the
##'     production processing algorithms. See
##'     \code{defaultProcessingParameters} for a starting point.
##' @param productionParams The production imputation parameter
##'     returned by the function \code{getImputationParameters}.
##'
##' @return A data.table containing the keys where the value should be
##'     forced to be zero.
##'
##' @export
##'



getForcedZeroKey = function(data,
                            processingParams =
                                defaultProcessingParameters(),
                            productionParams =
                                getImputationParameters()$productionParams
                            ){

    assumedZero =
        data[(get(processingParams$productionValue) == 0 |
              is.na(get(processingParams$productionValue))) &
             get(processingParams$productionMethodFlag) %in%
             c("-", "n") &
             get(processingParams$productionObservationFlag) == "M",
             c(processingParams$yearValue, productionParams$byKey),
             with = FALSE]
    ## Production must be zero if area harvested is 0.
    zeroProd =
        data[get(processingParams$productionValue) == 0 &
             get(processingParams$productionObservationFlag) != "M",
             c(processingParams$yearValue, productionParams$byKey),
             with = FALSE]
    zeroArea =
        data[get(processingParams$areaHarvestedValue) == 0 &
             get(processingParams$areaHarvestedObservationFlag) != "M",
             c(processingParams$yearValue, productionParams$byKey),
             with = FALSE]
    forcedZero = unique(rbind(assumedZero, zeroProd, zeroArea))
    forcedZero
}
