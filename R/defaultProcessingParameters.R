##' Default Processing Parameters
##' 
##' This function can be used to generate the input parameters for the data 
##' pre-processing code.  This is a good way to get a list of the required 
##' parameters and then modify parameters to match your particular 
##' configuration.
##' 
##' @param productionValue The element code corresponding to the production 
##'   variable.
##' @param yieldValue The element code corresponding to the yield variable.
##' @param areaHarvestedValue The element code corresponding to the area 
##'   harvested variable.
##'   
##' @return Returns a list of the default parameters used in the data 
##'   pre-processing algorithm.
##'   
##' @details Below is a description of the parameters: \itemize{
##' \item productionValue: The column name of the production variable.
##' \item productionObservationFlag: The column name of the observation flag 
##'   corresponding to the production variable.
##' \item productionMethodFlag: The column name of the method flag corresponding
##'   to the production variable.
##' \item yieldValue: The column name of the yield variable.
##' \item 
##'   yieldObservationFlag: The column name of the observation flag 
##'   corresponding to the yield variable.
##' \item yieldMethodFlag: The column 
##'   name of the method flag corresponding to the yield variable.
##' \item 
##'   areaHarvestedValue: The column name of the area harvested variable.
##' \item areaHarvestedObservationFlag: The column name of the observation flag 
##'   corresponding to the area harvested variable. 
##' \item areaHarvestedMethodFlag: The column name of the method flag
##'   corresponding to the area harvested variable.
##' \item yearValue: The column name for the year variable in data.
##' \item byKey: The column name for the variable 
##'   representing the splitting group.  Usually, this is the country variable. 
##' \item removePriorImputation: Should previous imputations be removed?
##' \item removeConflictValues: Should values which are marked as missing but
##' non-zero be removed?
##' \item imputedFlag: What flag should be filled in for imputed values?
##' \item naFlag: How are missing values specified in the database? Usually,
##'   this is "M".
##' }
##'   
##' @export
##' 

defaultProcessingParameters = function(productionValue = "5510",
                                       yieldValue = "5416",
                                       areaHarvestedValue = "5312"){
    list(productionValue = paste0("Value_measuredElement_",
                                  productionValue),
         productionObservationFlag = paste0("flagObservationStatus_measuredElement_",
                                            productionValue),
         productionMethodFlag = paste0("flagMethod_measuredElement_",
                                       productionValue),
         yieldValue = paste0("Value_measuredElement_", yieldValue),
         yieldObservationFlag = paste0("flagObservationStatus_measuredElement_", yieldValue),
         yieldMethodFlag = paste0("flagMethod_measuredElement_", yieldValue),
         areaHarvestedValue = paste0("Value_measuredElement_",
                                     areaHarvestedValue),
         areaHarvestedObservationFlag = paste0("flagObservationStatus_measuredElement_",
                                               areaHarvestedValue),
         areaHarvestedMethodFlag = paste0("flagMethod_measuredElement_",
                                          areaHarvestedValue),
         yearValue = "timePointYears",
         byKey = "geographicAreaM49",
         removePriorImputation = TRUE,
         removeConflictValues = TRUE,
         imputedFlag = "E",
         naFlag = "M")
}