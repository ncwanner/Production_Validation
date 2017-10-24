#'computeTotStock
#'
#'
#' This function has been created to compute the totStock that has to be used as
#' reference to compue the take off rates and consequently the number of slaughtered  animals.
#' In order to compute the total animal stocks, trade has to be taken into account
#' 
#' @param data datataset containing the the numeber of animal stocks (trade not included)
#' @return Returns a dataset with the the tot stock 
#' 
#' @export


computeTotSlaughtered = function(data,tradeElements, FormulaParameters=animalFormulaParameters){

## Data quality checks    
## 1) Check if the columns are all there
## 2) This function works only if data are dernormalized

    
##tradeParameters
    tradeParameters=list(ValueImport=paste0("Value_measuredElement_",tradeElements[1] ),
                         flagObservationImport=paste0("flagObservationStatus_measuredElement_",tradeElements[1]),
                         flagMethodImport=paste0("flagMethod_measuredElement_",tradeElements[1]),
                         ValueExport=paste0("Value_measuredElement_",tradeElements[2] ),
                         flagObservationExport=paste0("flagObservationStatus_measuredElement_",tradeElements[2]),
                         flagMethodExport=paste0("flagMethod_measuredElement_",tradeElements[2]))


    data=restore0M(data, valueVars = "tradeParameters$ValueImport", flagVars = "tradeParameters$flagObservationImport")
    data=restore0M(data, valueVars = "tradeParameters$ValueExport", flagVars = "tradeParameters$flagObservationExport")

## if the trade does not Exist we assure it is ZERO (not missig)

## For just one commodity we do not have any data for trade, this means that the columns referring to trade are missing
## In theory there shold be some quality chekc at the begining
if(sum(c(tradeParameters[[1]],tradeParameters[[4]]) %in% colnames(data))!=0){

    data[is.na(get(tradeParameters$ValueImport)),tradeParameters$ValueImport:=0 ]
    data[is.na(get(tradeParameters$ValueExport)),tradeParameters$ValueExport:=0 ]
}

flagValidTable1=copy(flagValidTable)
flagValidTable1[flagObservationStatus=="I" & flagMethod=="c", Protected:=FALSE]
flagValidTable1[flagObservationStatus=="E" & flagMethod=="c", Protected:=FALSE]
##flagValidTable1[flagObservationStatus=="E" & flagMethod=="f", Protected:=FALSE]
flagValidTable1[flagObservationStatus=="E" & flagMethod=="h", Protected:=FALSE]
##flagValidTable1[flagObservationStatus=="T" & flagMethod=="p", Protected:=FALSE]
##flagValidTable1[flagObservationStatus=="T" & flagMethod=="h", Protected:=FALSE]
##flagValidTable1[flagObservationStatus=="T" & flagMethod=="-", Protected:=FALSE]

## Before computing the takeOffRate, replace the non -protected slaughtered animals value with NA,M,u
data=removeNonProtectedFlag(data, valueVar =animalFormulaParameters$areaHarvestedValue,
                       observationFlagVar= animalFormulaParameters$areaHarvestedObservationFlag,
                       methodFlagVar=animalFormulaParameters$areaHarvestedMethodFlag,
                       flagValidTable = flagValidTable1)



if(sum(c(tradeParameters[[1]],tradeParameters[[4]]) %in% colnames(data))!=0){

    data[,takeOffRate:= (get(animalFormulaParameters$areaHarvestedValue)+get(tradeParameters$ValueExport)-get(tradeParameters$ValueImport))/get(animalFormulaParameters$productionValue)]
}else{
    
    data[,takeOffRate:= get(animalFormulaParameters$areaHarvestedValue)/get(animalFormulaParameters$productionValue)]
}


data[takeOffRate==Inf,takeOffRate:=NA]

##In case we use trade: that was not validated yet
data=data[takeOffRate<0, takeOffRate:=NA]


##Associate a Flat to the new variable "TakeOffRate"
data[,TakeOffFlagObservationStatus:="M"]
##All those OffTake rate that are not NA should have the observation flag resulting from the aggregation of the two variables used to compute it: 
##animalStock and slaughteredAnimal.

data[!is.na(takeOffRate)& 
                     !is.na(get(animalFormulaParameters$areaHarvestedObservationFlag)) &
                     !is.na(get(animalFormulaParameters$productionObservationFlag)),
                     TakeOffFlagObservationStatus:=aggregateObservationFlag(get(animalFormulaParameters$areaHarvestedObservationFlag),
                                                                            get(animalFormulaParameters$productionObservationFlag))]

data[,TakeOffRateFlagMethod:="u"]
## I chose the "c" method flag just to "protect" the computed Off take rate
data[!is.na(takeOffRate),TakeOffRateFlagMethod:="c"]




## Impute the missing takeOffRates

takeOffImputationParamenters=defaultImputationParameters()
takeOffImputationParamenters$imputationValueColumn="takeOffRate"
takeOffImputationParamenters$imputationFlagColumn="TakeOffFlagObservationStatus"
takeOffImputationParamenters$imputationMethodColumn="TakeOffRateFlagMethod"
takeOffImputationParamenters$byKey=c("geographicAreaM49","measuredItemCPC")
takeOffImputationParamenters$estimateNoData=FALSE

##takeOffImputationParamenters$ensembleModels$defaultExp=NULL
##takeOffImputationParamenters$ensembleModels$defaultLogistic=NULL
##takeOffImputationParamenters$ensembleModels$defaultLoess=NULL
##takeOffImputationParamenters$ensembleModels$defaultSpline=NULL
##takeOffImputationParamenters$ensembleModels$defaultMars=NULL
##takeOffImputationParamenters$ensembleModels$defaultMixedModel=NULL
##takeOffImputationParamenters$ensembleModels$defaultMovingAverage=NULL
##takeOffImputationParamenters$ensembleModels$defaultArima=NULL
##takeOffImputationParamenters$ensembleModels$defaultLm=NULL


takeOffImputed=data[,.(geographicAreaM49, measuredItemCPC, timePointYears,takeOffRate, TakeOffFlagObservationStatus, TakeOffRateFlagMethod)]

takeOffImputed=removeNoInfo(takeOffImputed,"takeOffRate", "TakeOffFlagObservationStatus",
                  byKey = c("geographicAreaM49", "measuredItemCPC" ))


takeOffImputed=imputeVariable(takeOffImputed,
                              imputationParameters=takeOffImputationParamenters)


data[,takeOffRate:=NULL]
data[,TakeOffFlagObservationStatus:=NULL]
data[,TakeOffRateFlagMethod:=NULL]

##When I make the merge I can recuperate those series when the slaughtered existes but there are no info for stock numbers (e.g. :Pacific Is)

takeOffImputed=merge(data,takeOffImputed,
                 by=c("geographicAreaM49", "measuredItemCPC", "timePointYears"), all.x = TRUE)


##compute the new slaughtered

if(sum(c(tradeParameters[[1]],tradeParameters[[4]]) %in% colnames(data))!=0){
takeOffImputed[,newSlaughtered:=(takeOffRate * get(animalFormulaParameters$productionValue))+get(tradeParameters$ValueImport)-get(tradeParameters$ValueExport)]
}else{
    
takeOffImputed[,newSlaughtered:=(takeOffRate * get(animalFormulaParameters$productionValue))]  
}
## if we are running the module locally this two line are useful to visualizeintermediate output.
##ToSave=nameData("agriculture", "aproduction",takeOffImputed)
##write.csv(ToSave, paste0("C:/Users/Rosa/Desktop/LivestockFinalDebug/takeOffImputed",currentMeatItem,".csv"))


## I have already deleted all the Non protected values from the animalSlaughtered series, 
## actually all the values associated to "I,c"/"E,c"/"E,h"/"T,p"/"T,h"/"T,-" flag combinations 
## have been removed even if they were not originally protected.


##OverWrite the newSlaughter into the missing value of the slaughterd column:
##Be careful to not overwrite slaugheter that were (M,-)
takeOffImputed[is.na(get(animalFormulaParameters$areaHarvestedValue)) &
                   !is.na(newSlaughtered)&
                   animalFormulaParameters$areaHarvestedMethodFlag!="-",
               animalFormulaParameters$areaHarvestedObservationFlag:="I"]


takeOffImputed[is.na(get(animalFormulaParameters$areaHarvestedValue)) &
                   !is.na(newSlaughtered)&
                   animalFormulaParameters$areaHarvestedMethodFlag!="-",
               animalFormulaParameters$areaHarvestedMethodFlag:="e"]


takeOffImputed[is.na(get(animalFormulaParameters$areaHarvestedValue)) &
                   !is.na(newSlaughtered)&
                   animalFormulaParameters$areaHarvestedMethodFlag!="-",
               animalFormulaParameters$areaHarvestedValue:=newSlaughtered]

slaughteredParentData=takeOffImputed[,c("geographicAreaM49",
                                        "measuredItemCPC",
                                        "timePointYears",
                                        animalFormulaParameters$areaHarvestedValue,
                                        animalFormulaParameters$areaHarvestedObservationFlag,
                                        animalFormulaParameters$areaHarvestedMethodFlag), with=FALSE]

slaughteredParentData=normalise(slaughteredParentData)

return(slaughteredParentData)


}

