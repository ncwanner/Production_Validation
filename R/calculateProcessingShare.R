##' Function to compute Processing sharing: share that identify the quantity of primary availability 
##' that it is allocated in different productive processes.
##'
##' @param data data table containing all the columns to compute processingSharing
##' @param printSharesGraterThan1 It is TRUE if we want to save some 
##' @param params defaultProcessedItamParams parameters, object which contains the parameters  
##'
##' @export
##'


calculateProcessingShare=function(data, printSharesGraterThan1=FALSE, param){
    
##Check that data contains all the necessary columns    
stopifnot(c(param$geoVar, param$yearVar, param$childVar, param$parentVar,
            param$extractVar, param$shareDownUp ,params$value, param$availVar) %in% colnames(data))

data[, processingShare:=(((get(param$value)/get(param$extractVar))*get(param$shareDownUp))/get( param$availVar ))]
data[,param$processingShare:= (( get(params$value)/get (param$extractVar) )* get(param$shareDownUp) )/get((param$availVar))]    
    



##-------------------------------------------------------------------------------------------------------
##Deviate the negative agailability to be manually checked
if(printSharesGraterThan1){

processingShareGraterThan1=data[processingShare>1]
directory= "C:/Users/Rosa/Desktop/DERIVATIVES/unfeasibleProcessingshare/"
dir.create(paste0(directory, lev), recursive=TRUE)
write.csv(processingShareGraterThan1, paste0(directory,lev, "/",currentGeo, "processingShareGraterThan1",".csv"), sep=";",row.names = F)
}

##-------------------------------------------------------------------------------------------------------
##Here we should perform an imputation of the imputation on 
processingShareParamenters=defaultImputationParameters()
processingShareParamenters$imputationValueColumn="processingShare"
processingShareParamenters$imputationFlagColumn="processingShareFlagObservationStatus"
processingShareParamenters$imputationMethodColumn="processingShareFlagMethod"
processingShareParamenters$byKey=c("geographicAreaM49", "measuredItemChildCPC", "measuredItemParentCPC")
processingShareParamenters$estimateNoData=FALSE


processingShareParamenters$ensembleModels$defaultExp=NULL
##processingShareParamenters$ensembleModels$defaultLogistic=NULL
processingShareParamenters$ensembleModels$defaultLoess=NULL
processingShareParamenters$ensembleModels$defaultSpline=NULL
processingShareParamenters$ensembleModels$defaultMars=NULL
processingShareParamenters$ensembleModels$defaultMixedModel=NULL
##processingShareParamenters$ensembleModels$defaultMovingAverage=NULL
##processingShareParamenters$ensembleModels$defaultArima=NULL

##dataInpute=copy(data)
data[,processingShareFlagObservationStatus:="M"]
data[,processingShareFlagMethod:="u"]


data[!is.na(processingShare),processingShareFlagObservationStatus:="T"]
data[!is.na(processingShare),processingShareFlagMethod:="-"]

##Remove series with no data
counts = data[, sum(!is.na(processingShare)),
               by = c(processingShareParamenters$byKey)]
counts=counts[V1!=0]
counts=counts[,c(param$geoVar, param$childVar, param$parentVar), with=FALSE]
data=data[counts, ,on=c(param$geoVar, param$childVar, param$parentVar)]

## impute processingSharing
data=imputeVariable(data,processingShareParamenters )
return(data)

}

