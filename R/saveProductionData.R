##' Save Production Data
##' 
##' This function takes the a production data dataset and saves it back to the
##' database.
##' 
##' @param data The data.table object containing the seed data to be written to
##' the database.
##' @param areaHarvestedCode Character string containing the element code
##' corresponding to the area harvested variable.
##' @param yieldCode Character string containing the element code
##' corresponding to the yield variable.
##' @param productionCode Character string containing the element code
##' corresponding to the production variable.
##' @param verbose Should output be printed about the progress of the save
##' data?  Defaults to FALSE.
##' @param context The data context of data.  The production imputation module
##' expands most sessions, as it requires at least 15 years of data and all
##' countries.  Providing this context parameter ensures that only the data
##' relevant to the initial session is saved back to the database.
##' @param waitMode See faosws::SaveData.
##' @param normalized See faosws::SaveData.
##' 
##' @return No R objects are returned, as this functions purpose is solely to
##' write to the database.
##' 
##' @export
##' 

saveProductionData = function(data, areaHarvestedCode = "5312",
                              yieldCode = "5421", productionCode = "5510",
                              verbose = FALSE,
                              context = swsContext.datasets[[1]],
                              waitMode = "wait", normalized = FALSE){
    
    ## Data Quality Checks
    stopifnot(is(data, "data.table"))
    stopifnot(is(context, "DatasetKey"))
    
    ## Remove columns to match the database
    requiredColumns = c("geographicAreaM49", "measuredItemCPC",
                        "timePointYears")
    requiredCodes = c(areaHarvestedCode, yieldCode, productionCode)
    missingColumns = requiredColumns[!requiredColumns %in% colnames(data)]
    if(length(missingColumns) > 0)
        stop("Missing required columns, so data cannot be saved!  Missing:\n",
             paste0(missingColumns, collapse = "\n"))
    if(!normalized){
        additionalColumns = lapply(requiredCodes, function(x)
            paste0(c("Value_measuredElement_",
                     "flagObservationStatus_measuredElement_",
                     "flagMethod_measuredElement_"), x))
        requiredColumns = c(requiredColumns, do.call("c", additionalColumns))
        data = data[, requiredColumns, with = FALSE]
    } else {
        data = data[, c(requiredColumns, "measuredElement", "Value",
                        "flagObservationStatus", "flagMethod"), with = FALSE]
    }
    
    ## Filter the data by removing any invalid date/country combinations
    if(verbose)
        cat("Removing invalid date/country combinations from the dataset.\n")
    data = removeInvalidDates(data)
    
    ## Code edit: If a value is NA, we don't really know if it's 0Mu, 0Mn, or
    ## something else.  So, let's just not save it!
    ## Can't save NA's back to the database, so convert to 0M
#     if(!normalized){
#         for(code in c(areaHarvestedCode, yieldCode, productionCode)){
#             valName = paste0("Value_measuredElement_", code)
#             obsFlag = paste0("flagObservationStatus_measuredElement_", code)
#             methodFlag = paste0("flagMethod_measuredElement_", code)
#             data[is.na(get(valName)) | get(obsFlag) == "M",
#                  `:=`(c(valName, obsFlag, methodFlag),
#                       list(0, "M", "n"))]
#         }
#     } else {
#         data[is.na(Value) | flagObservationStatus == "M",
#                  `:=`(c("Value", "flagObservationStatus", "flagMethod"),
#                       list(0, "M", "n"))]
#     }
    
    ## Collapse to passed context
    data = data[geographicAreaM49 %in% context@dimensions$geographicAreaM49@keys &
                measuredItemCPC %in% context@dimensions$measuredItemCPC@keys &
                timePointYears %in% context@dimensions$timePointYears@keys, ]
        
#     ## Round the data
#     if(!normalized){
#         valCols = paste0("Value_measuredElement_",
#                          c(areaHarvestedCode, productionCode))
#         for(code in c(areaHarvestedCode, productionCode)){
#             data[get(paste0("flagObservationStatus_measuredElement_", code)) == "I" &
#                  get(paste0("flagObservationStatus_measuredElement_", code)) %in% c("i", "e"),
#                      c(paste0("Value_measuredElement_", code) :=
#                            sapply(get(paste0("Value_measuredElement_", code)),
#                                   roundResults)]
#         }
#     } else {
#         data[measuredElement %in% c(areaHarvestedCode, productionCode),
#              Value := sapply(Value, roundResults)]
#     }
    
    ## Save the data back
    if(verbose)
        cat("Attempting to write data back to the database.\n")
    warning("HACK!  Sorting because of SaveData issue!")
    attr(data, "sorted") = NULL
    if(nrow(data) >= 1 & !normalized){ # If invalid dates caused 0 rows, don't try to save.
        faosws::SaveData(domain = "agriculture",
                         dataset = "aproduction",
                         data = data,
                         normalized = FALSE,
                         waitMode = waitMode)
    } else if(nrow(data) >= 1){
        faosws::SaveData(domain = "agriculture",
                         dataset = "aproduction",
                         data = data,
                         normalized = TRUE,
                         waitMode = waitMode)
    }        
}