##' This function creates a table to determine whether a country and
##' element combinated time series should use include estimates in the
##' iputation process.
##'
##' TODO (Michael): This function should be expanded to include item as well.
##'
##' @param data A data.table object containing the data to be imputed.
##' @param yieldObsFlagVar The column corresponding to the observation
##'     flag of the yield.
##' @param prodObsFlagVar The column corresponding to the observation
##'     flag of the production.
##' @param impFlags The value of the observation flags which are
##'     considered to be imputed.
##' @param missFlag The value of the observation flag which
##'     corresponds to missing value.

useEstimateForTimeSeriesImputation = function(data,
                                              areaObsFlagVar,
                                              yieldObsFlagVar,
                                              prodObsFlagVar,
                                              impFlags = c("I", "E"),
                                              missFlags = "M",
                                              minObsForEst = 5){
    areaElementNum = gsub("[^0-9]", "", areaObsFlagVar)
    yieldElementNum = gsub("[^0-9]", "", yieldObsFlagVar)
    prodElementNum = gsub("[^0-9]", "", prodObsFlagVar)
    validObsCnt =
        data[, list(area = sum(!get(areaObsFlagVar) %in%
                                c(impFlags, missFlags)),
                    yield = sum(!get(yieldObsFlagVar) %in%
                                c(impFlags, missFlags)),
                    prod = sum(!get(prodObsFlagVar) %in%
                               c(impFlags, missFlags))),
             by = "geographicAreaM49"]
    validObsCnt = melt(validObsCnt, id.vars = "geographicAreaM49")
    validObsCnt[, useEstimates := value < minObsForEst]
    validObsCnt[, measuredElement :=
                      ifelse(variable == "area", areaElementNum,
                      ifelse(variable == "yield", yieldElementNum,
                             prodElementNum))]
    validObsCnt[, c("variable", "value") := NULL]
    validObsCnt
}
