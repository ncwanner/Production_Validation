#'computeTotStock
#'
#'
#' This function has been created to compute the totStock that has to be used as
#' reference to compue the take off rates and consequently the number of slaughtered  animals.
#' In order to compute the total animal stocks, trade has to be taken into account
#' 
#' @param data datataset containing the the numeber of animal stocks (trade not included)
#' @param plot logical it says if we want to obtain the plot
#' @return Returns a dataset with the the tot stock 
#' 
#' @export
#'

computeTotSlaughtered = function(data,tradeElements, FormulaParameters=animalFormulaParameters, plot=TRUE){

## Data quality checks    
## 1) Check if the columns are all there
## 2) This function works only if data are dernormalized

    
##    tradeParameters
    tradeParameters=list(ValueImport=paste0("Value_measuredElement_",tradeElements[1] ),
                         flagObservationImport=paste0("flagObservationStatus_measuredElement_",tradeElements[1]),
                         flagMethodImport=paste0("flagMethod_measuredElement_",tradeElements[1]),
                         ValueExport=paste0("Value_measuredElement_",tradeElements[2] ),
                         flagObservationExport=paste0("flagObservationStatus_measuredElement_",tradeElements[2]),
                         flagMethodExport=paste0("flagMethod_measuredElement_",tradeElements[2]))
    
    
 

stockTrade=restore0M(stockTrade, valueVars = "tradeParameters$ValueImport", flagVars = "tradeParameters$flagObservationImport")
stockTrade=restore0M(stockTrade, valueVars = "tradeParameters$ValueExport", flagVars = "tradeParameters$flagObservationExport")

## if the trade does not Exist we assure it is ZERO (not missig)

## For just one commodity we do not have any data for trade, this means that the columns referring to trade are missing
## In theory there shold be some quality chekc at the begining
if(sum(c(tradeParameters[[1]],tradeParameters[[4]]) %in% colnames(stockTrade))!=0){

stockTrade[is.na(get(tradeParameters$ValueImport)),tradeParameters$ValueImport:=0 ]
stockTrade[is.na(get(tradeParameters$ValueExport)),tradeParameters$ValueExport:=0 ]
}

flagValidTable1=copy(flagValidTable)
flagValidTable1[flagObservationStatus=="I" & flagMethod=="c", Protected:=FALSE]
flagValidTable1[flagObservationStatus=="E" & flagMethod=="c", Protected:=FALSE]
##flagValidTable1[flagObservationStatus=="E" & flagMethod=="f", Protected:=FALSE]
flagValidTable1[flagObservationStatus=="E" & flagMethod=="h", Protected:=FALSE]
##flagValidTable1[flagObservationStatus=="T" & flagMethod=="p", Protected:=FALSE]
##flagValidTable1[flagObservationStatus=="T" & flagMethod=="h", Protected:=FALSE]
##flagValidTable1[flagObservationStatus=="T" & flagMethod=="-", Protected:=FALSE]
flagValidTable1[flagMethod=="h", Protected:=FALSE]



## Before computing the takeOffRate, replace the non -protected slaughtered animals value with NA,M,u
stockTrade=removeNonProtectedFlag(stockTrade, valueVar =animalFormulaParameters$areaHarvestedValue,
                       observationFlagVar= animalFormulaParameters$areaHarvestedObservationFlag,
                       methodFlagVar=animalFormulaParameters$areaHarvestedMethodFlag,
                       flagValidTable = flagValidTable1)



if(sum(c(tradeParameters[[1]],tradeParameters[[4]]) %in% colnames(stockTrade))!=0){

stockTrade[,takeOffRate:= (get(animalFormulaParameters$areaHarvestedValue)+get(tradeParameters$ValueExport)-get(tradeParameters$ValueImport))/get(animalFormulaParameters$productionValue)]
}else{
    
    stockTrade[,takeOffRate:= get(animalFormulaParameters$areaHarvestedValue)/get(animalFormulaParameters$productionValue)]
}


stockTrade[takeOffRate==Inf,takeOffRate:=NA]
stockTrade=stockTrade[takeOffRate<0, takeOffRate:=NA]



stockTrade[,TakeOffFlagObservationStatus:="M"]

stockTrade[!is.na(takeOffRate)& 
                     !is.na(get(animalFormulaParameters$areaHarvestedObservationFlag)) &
                     !is.na(get(animalFormulaParameters$productionObservationFlag)),
                     TakeOffFlagObservationStatus:=aggregateObservationFlag(get(animalFormulaParameters$areaHarvestedObservationFlag),
                                                                            get(animalFormulaParameters$productionObservationFlag))]

stockTrade[,TakeOffRateFlagMethod:="u"]
stockTrade[!is.na(takeOffRate),TakeOffRateFlagMethod:="c"]




## Impute the missing takeOffRates

takeOffImputationParamenters=defaultImputationParameters()
takeOffImputationParamenters$imputationValueColumn="takeOffRate"
takeOffImputationParamenters$imputationFlagColumn="TakeOffFlagObservationStatus"
takeOffImputationParamenters$imputationMethodColumn="TakeOffRateFlagMethod"
takeOffImputationParamenters$byKey=c("geographicAreaM49","measuredItemCPC")
takeOffImputationParamenters$estimateNoData=FALSE

takeOffImputationParamenters$ensembleModels$defaultExp=NULL
takeOffImputationParamenters$ensembleModels$defaultLogistic=NULL
takeOffImputationParamenters$ensembleModels$defaultLoess=NULL
takeOffImputationParamenters$ensembleModels$defaultSpline=NULL
takeOffImputationParamenters$ensembleModels$defaultMars=NULL
takeOffImputationParamenters$ensembleModels$defaultMixedModel=NULL
takeOffImputationParamenters$ensembleModels$defaultMovingAverage=NULL
takeOffImputationParamenters$ensembleModels$defaultArima=NULL
##takeOffImputationParamenters$ensembleModels$defaultLm=NULL



stockTrade=removeNoInfo(stockTrade,"takeOffRate", "TakeOffFlagObservationStatus",
                        byKey = c("geographicAreaM49", "measuredItemCPC" ))

takeOffImputed=imputeVariable(stockTrade,
                              imputationParameters=takeOffImputationParamenters)

## this is just to make an intermediate save of the takeOffRate
takeOffImputed[,.(geographicAreaM49, timePointYears,takeOffRate,TakeOffFlagObservationStatus,TakeOffRateFlagMethod )]
##write.csv(takeOffImputed, paste0("C:/Users/Rosa/Desktop/LivestockFinalDebug/takeOffImputed",currentMeatItem,".csv"))



##compute the new slaughtered

if(sum(c(tradeParameters[[1]],tradeParameters[[4]]) %in% colnames(stockTrade))!=0){
takeOffImputed[,newSlaughtered:=(takeOffRate * get(animalFormulaParameters$productionValue))+get(tradeParameters$ValueImport)-get(tradeParameters$ValueExport)]
}else{
    
takeOffImputed[,newSlaughtered:=(takeOffRate * get(animalFormulaParameters$productionValue))]  
}


###plot

if(plot){
    
    library(faosws)
    library(faoswsUtil)
    library(data.table)
    library(igraph)
    library(faoswsBalancing)
    library(faoswsStandardization)
    library(dplyr)
    library(MASS) 
    library(lattice)
    library(reshape2)
    library(forecast)
    library(tidyr)
    
    
    
    countryName=nameData("agriculture", "aproduction", takeOffImputed[,.(geographicAreaM49)])
    countryName=unique(countryName)
    
    takeOffImputedLabel=merge(takeOffImputed,countryName, by="geographicAreaM49",all.x=TRUE)
    
    
    
    if(sum(c(tradeParameters[[1]],tradeParameters[[4]]) %in% colnames(stockTrade))!=0){
    data_melt <- melt(takeOffImputedLabel,id.vars = c("geographicAreaM49",
                                                      "measuredItemCPC",
                                                      "timePointYears",
                                                      "geographicAreaM49_description"   ),
                      measure.vars=c(animalFormulaParameters$productionValue,
                                     animalFormulaParameters$areaHarvestedValue,
                                     tradeParameters$ValueImport,
                                     tradeParameters$ValueExport,
                                     "newSlaughtered"),
                      value.name = "Value")
    
    }else{
        data_melt <- melt(takeOffImputedLabel,id.vars = c("geographicAreaM49",
                                                          "measuredItemCPC",
                                                          "timePointYears",
                                                          "geographicAreaM49_description"   ),
                          measure.vars=c(animalFormulaParameters$productionValue,
                                         animalFormulaParameters$areaHarvestedValue,
                                         "newSlaughtered"),
                          value.name = "Value")
        
        
    }
    
    
    pdf(paste0("C:/Users/Rosa/Desktop/LivestockFinalDebug/chartSlaughtered",currentMeatItem,".pdf"),
        paper = "a4",width=9, height=15)  
    
    geo=unique(data_melt[, geographicAreaM49])
    pnp=list()
    
    
    
    pags = 12
    nump <- seq(1,length(geo),pags)
    if(length(geo)/pags==length(geo)%/%pags){
        lgp=length(geo)/pags}else{
            lgp=length(geo)%/%pags+1
        }
    
    for(i in 1:lgp){
        if(!is.na(nump[i+1])){
            pnp[[i]] <-  ggplot(data_melt[geographicAreaM49 %in% geo[nump[i]:nump[i+1]-1]], aes(x=timePointYears, y=Value)) + 
                geom_line(aes(linetype=variable,color=variable,size=variable)) + 
                scale_x_continuous(breaks=2000:2014) +
                scale_linetype_manual(values=c("solid","longdash","dotdash","solid","solid")) +
                scale_colour_manual(values = c("black","blue","red","yellow","green")) +
                scale_size_manual(values=c(0.3,0.5,0.3,0.3,0.3)) +
                theme(axis.title =element_text(size=5),
                      axis.text.y = element_text(size=5),
                      axis.text.x = element_text(size=4,angle = 50, hjust = 1),
                      legend.text = element_text(size=6),
                      strip.text.x = element_text(size = 7),
                      legend.position = "top",
                      panel.grid.major = element_line(colour = "grey80", linetype = "longdash",size= 0.2 ), 
                      panel.grid.minor = element_line(colour="white",size=0), 
                      panel.background = element_rect(fill="white")) +
                facet_wrap(~geographicAreaM49_description, ncol = 3,scales = "free")
            print(pnp[[i]])
            
        }else{
            if(!is.na(nump[i])){
                pnp[[i]] <-  ggplot(data_melt[geographicAreaM49 %in% geo[nump[i]:length(geo)]], aes(x=timePointYears, y=Value)) + 
                    geom_line(aes(linetype=variable,color=variable,size=variable)) + 
                    scale_x_continuous(breaks=2000:2013) +
                    scale_linetype_manual(values=c("solid","longdash","dotdash","solid","solid")) +
                    scale_colour_manual(values = c("black","blue","red","yellow","green")) +
                    scale_size_manual(values=c(0.3,0.5,0.3,0.3,0.3)) +
                    theme(axis.title =element_text(size=5),
                          axis.text.y = element_text(size=5),
                          axis.text.x = element_text(size=4,angle = 50, hjust = 1),
                          legend.text = element_text(size=6),
                          strip.text.x = element_text(size = 7),
                          legend.position = "top",
                          panel.grid.major = element_line(colour = "grey80", linetype = "longdash",size= 0.2 ), 
                          panel.grid.minor = element_line(colour="white",size=0), 
                          panel.background = element_rect(fill="white")) +
                    facet_wrap(~geographicAreaM49_description, ncol = 3,scales = "free")
                print(pnp[[i]])
                
            }}}
    
    
    dev.off()
}





takeOffImputed[,flagComb:= paste(   get(animalFormulaParameters$areaHarvestedObservationFlag),  
                                    get(animalFormulaParameters$areaHarvestedMethodFlag), sep=";" )]



## I have already deleted all the Non protected values from the animalSlaughtered series, 
## actually all the values associated to "I,c"/"E,c"/"E,h"/"T,p"/"T,h"/"T,-" flag combinations 
## have been removed even if they were not originally proected.


##NonProtectedFlag = faoswsFlag::flagValidTable[Protected == FALSE,]
##NonProtectedFlag= NonProtectedFlag[, combination := paste(flagObservationStatus, flagMethod, sep = ";")]
##NonProtectedFlagComb=NonProtectedFlag[,combination]






takeOffImputed[is.na(get(animalFormulaParameters$areaHarvestedValue)) & !is.na(newSlaughtered),
               animalFormulaParameters$areaHarvestedObservationFlag:="I"]
##takeOffImputed[get(animalFormulaParameters$areaHarvestedObservationFlag)=="M" & !is.na(newSlaughtered),
##               animalFormulaParameters$areaHarvestedObservationFlag:=""]


takeOffImputed[is.na(get(animalFormulaParameters$areaHarvestedValue)) & !is.na(newSlaughtered),
               animalFormulaParameters$areaHarvestedMethodFlag:="e"]
##takeOffImputed[get(animalFormulaParameters$areaHarvestedObservationFlag)=="M" & !is.na(newSlaughtered),
##               animalFormulaParameters$areaHarvestedMethodFlag:="h"]



takeOffImputed[is.na(get(animalFormulaParameters$areaHarvestedValue)) & !is.na(newSlaughtered),
               animalFormulaParameters$areaHarvestedValue:=newSlaughtered]
##takeOffImputed[get(animalFormulaParameters$areaHarvestedObservationFlag)=="M" & !is.na(newSlaughtered),
##               animalFormulaParameters$areaHarvestedValue:=newSlaughtered]



slaughteredParentData=takeOffImputed[,.(geographicAreaM49,
                                        measuredItemCPC,
                                        timePointYears,
                                        get(animalFormulaParameters$areaHarvestedValue),
                                        get(animalFormulaParameters$areaHarvestedObservationFlag),
                                        get(animalFormulaParameters$areaHarvestedMethodFlag))]

setnames(slaughteredParentData, c("V4", "V5", "V6"), c(animalFormulaParameters$areaHarvestedValue,
                                                       animalFormulaParameters$areaHarvestedObservationFlag,
                                                       animalFormulaParameters$areaHarvestedMethodFlag))


slaughteredParentData=normalise(slaughteredParentData)




return(slaughteredParentData)


}

