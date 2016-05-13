##' Function to compute area harvested when new production and yield are given.
##'
##' @param data The data.table object containing the data.
##' @param processingParameters A list of the parameters for the production
##'     processing algorithms. See \code{productionProcessingParameters} for a
##'     starting point.
##' @param unitConversion Yield is computed as (production) / (area) and
##'     multiplied by unitConversion. This parameter defaults to 1.
##'
##' @export
##'

balanceAreaHarvested = function(data,
                                processingParameters,
                                unitConversion = 1){

    ## Data Quality Checks
    if(!exists("ensuredProductionData") || !ensuredProductionData)
        ensureProductionInputs(data = data,
                               processingParameters = processingParameters)

    ## Save clutter by renaming "processingParameters" to "p" locally.
    param = processingParameters

    ## Impute only when area and yield are available and production isn't
    filter = data[,is.na(get(param$areaHarvestedValue)) & # area is missing
                   !is.na(get(param$yieldValue)) &        # yield is available
                   !is.na(get(param$productionValue))]    # production is available
    filter2 = data[,get(param$productionObservationFlag) != param$missingValueObservationFlag &
                    get(param$yieldObservationFlag) != param$missingValueObservationFlag]
    filter = filter & filter2

    data[filter, `:=`(c(param$areaHarvestedValue),
                      sapply(computeRatio(get(param$productionValue),
                                          get(param$yieldValue)) *
                             unitConversion, roundResults))]

    data[filter,
         `:=`(c(param$areaHarvestedObservationFlag),
              aggregateObservationFlag(get(param$productionObservationFlag),
                                       get(param$yieldObservationFlag)))]
    ## If production is zero, then the area harvested should be
    ## zero. Sometimes this is not calculated as yield can be missing.
    data[get(param$productionValue) == 0 &
         get(param$productionObservationFlag) != param$missingValueObservationFlag &
         !(get(param$areaHarvestedMethodFlag) %in% param$protectedMethodFlag),
         `:=`(c(param$areaHarvestedValue, param$areaHarvestedObservationFlag,
                param$areaHarvestedMethodFlag),
              list(0, param$imputationObservationFlag, param$balanceMethodFlag))]


    ## Wrap last call in invisible() so no data.table is returned
    invisible(data[filter, `:=`(c(param$areaHarvestedMethodFlag),
                                param$imputationMethodFlag)])
}
