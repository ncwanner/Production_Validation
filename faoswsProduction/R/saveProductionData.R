##' Save Production Data
##' 
##' This function takes the a seed data dataset and saves it back to the
##' database.
##' 
##' @param data The data.table object containing the seed data to be written to
##' the database.
##' 
##' @return No R objects are returned, as this functions purpose is solely to
##' write to the database.
##' 
##' @export
##' 

saveProductionData = function(data){
    
    ## Data Quality Checks
    stopifnot(is(data, "data.table"))
    
    ## Remove columns to match the database
    requiredColumns = c("geographicAreaM49", "measuredItemCPC",
                        "timePointYears", "Value_measuredElement_5312",
                        "flagObservationStatus_measuredElement_5312",
                        "flagMethod_measuredElement_5312",
                        "Value_measuredElement_5416",
                        "flagObservationStatus_measuredElement_5416",
                        "flagMethod_measuredElement_5416",
                        "Value_measuredElement_5510",
                        "flagObservationStatus_measuredElement_5510",
                        "flagMethod_measuredElement_5510")
    data = data[, requiredColumns, with = FALSE]
    missingColumns = requiredColumns[!requiredColumns %in% colnames(data)]
    if(length(missingColumns) > 0)
        stop("Missing required columns, so data cannot be saved!  Missing:\n",
             paste0(missingColumns, collapse = "\n"))
    
    ## Filter the data by removing any invalid date/country combinations
    data = faoswsUtil::removeInvalidDates(data)
        
    ## Save the data back
    faosws::SaveData(domain = "agriculture",
                     dataset = "agriculture",
                     data = data,
                     normalized = FALSE)
}