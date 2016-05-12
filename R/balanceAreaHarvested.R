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
    p = processingParameters

    ## Impute only when area and yield are available and production isn't
    filter = data[,is.na(get(p$areaHarvestedValue)) & # area is missing
                  !is.na(get(p$yieldValue)) &         # yield is available
                  !is.na(get(p$productionValue))]     # production is available
    filter2 = data[,get(p$productionObservationFlag) != param$missingValueObservationFlag &
                    get(p$yieldObservationFlag) != param$missingValueObservationFlag]
    filter = filter & filter2

    data[filter, `:=`(c(p$areaHarvestedValue),
                      sapply(computeRatio(get(p$productionValue),
                                          get(p$yieldValue)) *
                             unitConversion, roundResults))]

    data[filter,
         `:=`(c(p$areaHarvestedObservationFlag),
              aggregateObservationFlag(get(p$productionObservationFlag),
                                       get(p$yieldObservationFlag)))]
    ## If production is zero, then the area harvested should be
    ## zero. Sometimes this is not calculated as yield can be missing.
    data[get(p$productionValue) == 0 &
         get(p$productionObservationFlag) != p$naFlag &
         !(get(p$areaHarvestedMethodFlag) %in% param$protectedMethodFlag),
         `:=`(c(p$areaHarvestedValue, p$areaHarvestedObservationFlag,
                p$areaHarvestedMethodFlag),
              list(0, param$imputationObservationFlag, param$balanceMethodFlag))]


    ## Wrap last call in invisible() so no data.table is returned
    invisible(data[filter, `:=`(c(p$areaHarvestedMethodFlag),
                                param$imputationMethodFlag)])
}
