## NOTE (Michael): This function should probably be merged with
##                 processing parameters.
getFormulaPrefix = function(){
    formulaPrefix =
        data.table(
            valuePrefix = "Value_measuredElement_",
            flagObsPrefix = "flagObservationStatus_measuredElement_",
            flagMethodPrefix = "flagMethod_measuredElement_"
        )
}
