allCountryCodes = GetCodeList(domain = "agriculture",
                              dataset = "aproduction",
                              dimension = areaVar)
allCountryCodes = unique(allCountryCodes[type == "country", code])
## HACK: Update China
warning("Hack below!  Remove once the geographicAreaM49 dimension is fixed!")
allCountryCodes = c(allCountryCodes, "158", "1248")
allCountryCodes = unique(allCountryCodes)

allItemCodes = GetCodeList(domain = "agriculture",
                           dataset = "aproduction",
                           dimension = "measuredItemCPC")
allItemCodes = unique(allItemCodes[!is.na(type), code])
allItemCodes = allItemCodes[!is.na(allItemCodes)]

yieldFormula = GetCodeTree(domain = "agriculture",
                           dataset = "aproduction",
                           dimension = "measuredElement")
productionElements = yieldFormula[parent %in% c("31", "41", "51"),
                                  do.call("c", strsplit(children, ", "))]
productionElements = unique(productionElements)

key = DatasetKey(
    domain = "agriculture",
    dataset = "aproduction",
    dimensions = list(
        geographicAreaM49 = Dimension(name = "geographicAreaM49",
                                      keys = allCountryCodes),
        measuredElement = Dimension(name = "measuredElement",
                                    keys = productionElements),
        measuredItemCPC = Dimension(name = "measuredItemCPC",
                                    keys = allItemCodes),
        timePointYears = Dimension(name = "timePointYears",
                                   keys = as.character(years)) # 15 years
        )
    )

allProd = GetData(key)
itemType = GetCodeList(domain = "agriculture", dataset = "aproduction",
                       dimension = "measuredItemCPC")[, .(code, type)]
itemType = itemType[!is.na(type), ]
setnames(itemType, "code", "measuredItemCPC")
allProd[itemType, type := type, on = "measuredItemCPC"]
finalData = allProd[, .N, by = c("measuredElement", "type")]
out = dcast(finalData, type ~ measuredElement, fun.aggregate = sum, value.var = "N")
write.csv(out, file = "~/Documents/Github/faoswsProduction/sandbox/elementsByItemType.csv",
          row.names = FALSE)