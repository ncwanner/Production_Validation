##' Get Main Key
##' 
##' This function generates a DatasetKey object (see faosws definition) which is
##' used to query the production dataset.
##' 
##' @param years A numeric vector of the years which should be extracted and
##'   used in this analysis.
##'   
##' @return A DatasetKey object.
##' 
##' @export
##' 

getMainKey = function(years){
    allCountryCodes = GetCodeList(domain = "agriculture",
                                  dataset = "aproduction",
                                  dimension = "geographicAreaM49")
    allCountryCodes = unique(allCountryCodes[type == "country", code])
    ## HACK: Update China
    warning("Hack below!  Remove once the geographicAreaM49 dimension is fixed!")
    allCountryCodes = c(allCountryCodes, "158", "1248")
    allCountryCodes = unique(allCountryCodes)
    
    allItemCodes = GetCodeList(domain = "agriculture", dataset = "aproduction",
                               dimension = "measuredItemCPC")
    allItemCodes = unique(allItemCodes[!is.na(type), code])
    
    # List of primary codes from Nic Sakoff:
    allPrimaryCodes = fread("~/Documents/Github/faoswsProduction/sandbox/codesToDisseminate.csv",
                            colClasses = "char")
    # List of all disseminated codes from FAOSTAT: (really need a better way of
    # doing this...)
    allPrimaryCodes = fread("~/Documents/Github/faoswsProduction/sandbox/allPrimaryCodes.csv",
                            colClasses = "char")
    allPrimaryCodes = allPrimaryCodes[, measuredItemCPC]
    ## Derived codes provided by Tomasz:
    allDerivedCodes = c(306, 307, 1242, 162, 165, 237, 244, 252, 256, 257, 258,
                        261, 268, 271, 281, 290, 329, 331, 334, 51, 564, 60, 767,
                        1745, 1809, 1811, 1816, 1021, 1022, 1043, 1186, 1225, 885,
                        886, 887, 888, 889, 890, 891, 894, 895, 896, 897, 898, 899,
                        900, 901, 904, 952, 953, 955, 983, 984)
    allDerivedCodes = faoswsUtil::fcl2cpc(formatC(allDerivedCodes, width = 4, flag = "0"))
    allItemCodes = c(allPrimaryCodes, allDerivedCodes)
    allItemCodes = unique(allItemCodes[!is.na(allItemCodes)])
    # For testing purposes:
    # allItemCodes = swsContext.datasets[[1]]@dimensions$measuredItemCPC@keys
    # allItemCodes = GetCodeList("agriculture", "aproduction", "measuredItemCPC")[type == "LSPR", code]
    # allItemCodes = grep("011.*", allItemCodes, value = TRUE)
    
    yieldFormula = ReadDatatable(table = "item_type_yield_elements")
    productionElements = unique(unlist(yieldFormula[, list(element_31, element_41,
                                                           element_51)]))
    
    fullKey = DatasetKey(
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
}