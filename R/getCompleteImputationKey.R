##' This function reads the complete imputation key from the Datatable
##' 'fbs_production_comm_codes'
##'
##' @return A DatasetKey object containing all the keys for production
##'     imputation
##'
##' @export

getCompleteImputationKey = function(){
    completeImputationCodes = ReadDatatable("fbs_production_comm_codes")
    allCountryCodes =
        completeImputationCodes[fbs_key == "geographicAreaM49", fbs_code]
    allItemCodes =
        completeImputationCodes[fbs_key == "measuredItemCPC", fbs_code]
    allElementCodes =
        completeImputationCodes[fbs_key == "measuredElement", fbs_code]
    allYearCodes =
        completeImputationCodes[fbs_key == "timePointYears", fbs_code]
    completeImputationKey =
        DatasetKey(domain = "agriculture",
                   dataset = "aproduction",
                   dimensions =
                       list(geographicAreaM49 =
                                Dimension(name = "geographicAreaM49",
                                          keys = allCountryCodes),
                            measuredItemCPC =
                                Dimension(name = "measuredItemCPC",
                                          keys = allItemCodes),
                            measuredElement =
                                Dimension(name = "measuredElement",
                                          keys = allElementCodes),
                            timePointYears =
                                Dimension(name = "timePointYears",
                                          keys = allYearCodes)))
    completeImputationKey
}
