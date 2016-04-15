normalise = function(denormalisedData,
                     areaVar = "geographicAreaM49",
                     yearVar = "timePointYears",
                     itemVar = "measuredItemCPC",
                     elementVar = "measuredElement",
                     flagObsVar = "flagObservationStatus",
                     flagMethodVar = "flagMethod",
                     valueVar = "Value"){

    measuredTriplet = c(valueVar, flagObsVar, flagMethodVar)
    allKey = c(areaVar, itemVar, elementVar, yearVar)
    ## denormalisedData = copy(step2Data)
    normalisedKey = intersect(allKey, colnames(denormalisedData))
    denormalisedKey = setdiff(allKey, normalisedKey)
    normalisedList =
        lapply(measuredTriplet,
               FUN = function(x){
                   splitDenormalised =
                       denormalisedData[, c(normalisedKey,
                                            grep(x, colnames(denormalisedData),
                                                 value = TRUE)),
                                        with = FALSE]
                   splitNormalised =
                       melt(splitDenormalised, id.vars = normalisedKey,
                            variable.name = denormalisedKey, value.name = x)
                   substitutePattern = paste0("(", x, "|", denormalisedKey, "|_)")
                   splitNormalised[, `:=`(c(denormalisedKey),
                                          gsub(substitutePattern, "",
                                               .SD[[denormalisedKey]]))]
                   setkeyv(splitNormalised,
                           col = c(normalisedKey, denormalisedKey))
               })

    normalisedData =
        Reduce(merge, x = normalisedList)

}
