denormalise = function(normalisedData,
                       denormaliseKey,
                       areaVar = "geographicAreaM49",
                       itemVar = "measuredItemCPC",
                       elementVar = "measuredElement",
                       yearVar = "timePointYears",
                       flagObsVar = "flagObservationStatus",
                       flagMethodVar = "flagMethod",
                       valueVar = "Value"){
    
    allKey = c(areaVar, itemVar, elementVar, yearVar)
    measuredTriplet = c(valueVar, flagObsVar, flagMethodVar)
    normaliseKey = setdiff(allKey, denormaliseKey)

    denormaliseFormula =
        as.formula(paste0(paste0(normaliseKey, collapse = " + "), " ~ ",
                          denormaliseKey))
    seperator = paste0("_", denormaliseKey, "_")


    denormalised = dcast(normalisedData, formula = denormaliseFormula,
                         value.var = measuredTriplet,
                 sep = seperator)
    denormalised = fillRecord(denormalised)

    uniqueElementCodes =
        unique(gsub("[^0-9]", "",
                    grep("[0-9]{4}", colnames(denormalised), value = TRUE)))

    setcolorder(x = denormalised,
                neworder = c("geographicAreaM49",
                             "measuredItemCPC",
                             "timePointYears",
                             sapply(uniqueElementCodes,
                                    FUN = function(x){
                                        grep(x, colnames(denormalised),
                                             value = TRUE)
                                    }
                                    )))
    denormalised
}
