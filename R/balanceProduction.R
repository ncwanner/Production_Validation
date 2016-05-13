##' Function to compute production when new area harvested and yield are given.
##'
##' @param data The data.table object containing the data.
##' @param processingParameters A list of the parameters for the
##'     production processing algorithms.  See
##'     \code{productionProcessingParameters} for a starting point.
##' @param unitConversion Yield is computed as (production) / (area)
##'     and multiplied by unitConversion.  This parameter defaults to
##'     1.
##'
##' @export
##'

balanceProduction = function(data,
                             processingParameters,
                             unitConversion = 1){

    ## Data Quality Checks
    if(!exists("ensuredProductionData") || !ensuredProductionData)
        ensureProductionInputs(data = data,
                               processingParameters = processingParameters)

    ## Type "param" instead of "processingParameters"
    param = processingParameters

    ## Impute only when area and yield are available and production isn't
    filter = data[,!is.na(get(param$areaHarvestedValue)) & # area is available
                   is.na(get(param$productionValue)) &    # production is missing
                   !is.na(get(param$yieldValue))]          # yield is missing
    filter2 = data[,get(param$areaHarvestedObservationFlag) !=
                    param$missingValueObservationFlag &
                    get(param$yieldObservationFlag) !=
                    param$missingValueObservationFlag]
    filter = filter & filter2

    data[filter, `:=`(c(param$productionValue),
                      sapply(get(param$areaHarvestedValue) *
                             get(param$yieldValue) /
                             unitConversion, FUN = roundResults))]
    data[filter,
         `:=`(c(param$productionObservationFlag),
              aggregateObservationFlag(get(param$areaHarvestedObservationFlag),
                                       get(param$yieldObservationFlag)))]
    ## Wrap last call in invisible() so no data.table is returned
    invisible(data[filter, `:=`(c(param$productionMethodFlag),
                                param$imputationMethodFlag)])
}
