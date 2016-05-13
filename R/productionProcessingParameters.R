##' Default Processing Parameters
##'
##' This function can be used to generate the input parameters for the data
##' pre-processing code.  This is a good way to get a list of the required
##' parameters and then modify parameters to match your particular
##' configuration.
##'
##' @param datasetConfig The dataset configuration returned by the function
##'     \code{GetDatasetConfig}
##' @param productionCode The element code corresponding to the production
##'     variable.
##' @param yieldCode The element code corresponding to the yield variable.
##' @param areaHarvestedCode The element code corresponding to the area
##'     harvested variable.
##' @param removePriorImputation logical, whether previous imputation values
##'     should be removed.
##' @param removeManualEstimation logical, whether previous manual estimation
##'     should be removed.
##' @param imputationObservationFlag The observation flag that represents
##'     imputation.
##' @param imputationMethodFlag The observation flag that represents estimated
##'     by statistical algorithm.
##' @param balanceMethodFlag The method flag that for calculation based on
##'     identity
##' @param manualEstimationObservationFlag The observation status flag that
##'     corresponds to manual (FAO) estimates
##' @param manualEstimationMethodFlag The method flag that corresponds to manual
##'     (FAO) estimates
##' @param missingValueObservationFlag The observation flag corresponding to
##'     missing values.
##' @param missingValueMethodFlag The method flag corresponding to missing
##'     values.
##' @param protectedMethodFlag The list of method flag that are considered as
##'     protected and should not be over-written.
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
##' \item yieldObservationFlag: The column name of the observation flag
##'   corresponding to the yield variable.
##' \item yieldMethodFlag: The column
##'   name of the method flag corresponding to the yield variable.
##' \item areaHarvestedValue: The column name of the area harvested variable.
##' \item areaHarvestedObservationFlag: The column name of the observation flag
##'   corresponding to the area harvested variable.
##' \item areaHarvestedMethodFlag: The column name of the method flag
##'   corresponding to the area harvested variable.
##' \item areaVar: The column corresponding to the geographic area.
##' \item yearVar: The column corresponding to the time dimension.
##' \item itemVar: The column corresponding to the item/commodity.
##' \item elementVar: The column corresponding to the measured element.
##' \item valueVar: The name for value column in the normalised form.
##' \item flagObservationVar: The name for observation flag in the normalised form.
##' \item flagMethod: The name for method flag in the normalised form.
##' \item removePriorImputation: Should previous imputations be removed?
##' \item removeManualEstimation: Should previous manual estimates be removed?
##' \item imputationObservationFlag: The flag for imputation.
##' \item imputationMethodFlag: The observation flag that represents estimated by statistical algorithm.
##' \item balanceMethodFlag: The method flag that for calculation based on identity
##' \item manualEstimationObservationFlag: The observation status flag that corresponds to manual (FAO) estimates
##' \item manualEstimationMethodFlag: The method flag that corresponds to manual (FAO) estimates
##' \item missingValueObservationFlag: The observation flag corresponding to missing values.
##' \item missingValueMethodFlag: The method flag corresponding to missing values.
##' \item protectedMethodFlag: The list of method flag that are considered as protected and should not be over-written.
##' }
##'
##' @export
##'

productionProcessingParameters = function(datasetConfig,
                                          productionCode = "5510",
                                          areaHarvestedCode = "5312",
                                          yieldCode = "5416",
                                          removePriorImputation = TRUE,
                                          removeManualEstimation = TRUE,
                                          imputationObservationFlag = "I",
                                          imputationMethodFlag = "e",
                                          balanceMethodFlag = "i",
                                          manualEstimationObservationFlag = "E",
                                          manualEstimationMethodFlag = "f",
                                          missingValueObservationFlag = "M",
                                          missingValueMethodFlag = "u",
                                          protectedMethodFlag = c("-", "q", "p", "h", "c")){
    ## HACK (Michael): There is no information on how to configure this, and
    ##                 thus it is hard coded.
    areaVar = datasetConfig$dimensions[1]
    itemVar = datasetConfig$dimensions[3]
    elementVar = datasetConfig$dimensions[2]
    yearVar = datasetConfig$timeDimension
    flagObservationVar = datasetConfig$flags[1]
    flagMethodVar = datasetConfig$flags[2]
    valueVar = "Value"

    ## Return the list of parameters
    list(productionValue =
             paste0(c(valueVar, elementVar, productionCode), collapse = "_"),
         areaHarvestedValue =
             paste0(c(valueVar, elementVar, areaHarvestedCode), collapse = "_"),
         yieldValue =
             paste0(c(valueVar, elementVar, yieldCode), collapse = "_"),
         productionObservationFlag =
             paste0(c(flagObservationVar, elementVar, productionCode),
                    collapse = "_"),
         areaHarvestedObservationFlag =
             paste0(c(flagObservationVar, elementVar, areaHarvestedCode),
                    collapse = "_"),
         yieldObservationFlag =
             paste0(c(flagObservationVar, elementVar, yieldCode), collapse = "_"),
         productionMethodFlag =
             paste0(c(flagMethodVar, elementVar, productionCode), collapse = "_"),
         areaHarvestedMethodFlag =
             paste0(c(flagMethodVar, elementVar, areaHarvestedCode),
                    collapse = "_"),
         yieldMethodFlag =
             paste0(c(flagMethodVar, elementVar, yieldCode), collapse = "_"),
         areaVar = areaVar,
         yearVar = yearVar,
         itemVar = itemVar,
         elementVar = elementVar,
         flagObservationVar = flagObservationVar,
         flagMethodVar = flagMethodVar,
         valueVar = valueVar,
         removePriorImputation = removePriorImputation,
         removeManualEstimation = removeManualEstimation,
         imputationObservationFlag = imputationObservationFlag,
         imputationMethodFlag = imputationMethodFlag,
         ## NOTE (Michael): balanceMethod should not have an observation flag,
         ##                 it uses flag aggregation.
         balanceMethodFlag = balanceMethodFlag,
         manualEstimationObservationFlag = manualEstimationObservationFlag,
         manualEstimationMethodFlag = manualEstimationMethodFlag,
         missingValueObservationFlag = missingValueObservationFlag,
         missingValueMethodFlag = missingValueMethodFlag,
         protectedMethodFlag = protectedMethodFlag
         )

}
