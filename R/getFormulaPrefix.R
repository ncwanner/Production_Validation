##' This function returns the prefix name used for the denormalised
##' dataset.
##'
##' TODO (Michael): This function should probably be merged with
##'                 processing parameters.
##'
##' @return A data.table containing the prefix
##'
##' @export

getFormulaPrefix = function(){
    formulaPrefix =
        data.table(
            valuePrefix = "Value_measuredElement_",
            flagObsPrefix = "flagObservationStatus_measuredElement_",
            flagMethodPrefix = "flagMethod_measuredElement_"
        )
    formulaPrefix    
}
