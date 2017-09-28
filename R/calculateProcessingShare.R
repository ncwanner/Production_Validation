##' Function to compute Processing sharing: share that identify the quantity of primary availability 
##' that it is allocated in different productive processes.
##'
##' @param data 
##' @param printSharesGraterThan1 
##' @param params   
##'
##' @export
##'




calculateProcessingShare=function(data, printSharesGraterThan1=FALSE){
    
##Check that data containd all the necessary columns    

    
    
    
data[, processingShare:=(((Value/extractionRate)*shareDownUp)/availability)]
data[,weight:=NULL]


##-------------------------------------------------------------------------------------------------------
##Deviate the negative agailability to be manually checked
if(printSharesGraterThan1){

processingShareGraterThan1=data[processingShare>1]
directory= "C:/Users/Rosa/Desktop/DERIVATIVES/unfeasibleProcessingshare/"
dir.create(paste0(directory, lev), recursive=TRUE)
write.csv(processingShareGraterThan1, paste0(directory,lev, "/",currentGeo, "processingShareGraterThan1",".csv"), sep=";",row.names = F)
}

##------------------------------------------------------------------------------------------------------



##-------------------------------------------------------------------------------------------------------------------------------------    
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


counts = data[,
               sum(!is.na(processingShare)),
               by = c(processingShareParamenters$byKey)]
counts[V1!=0]
counts=counts[V1!=0]
counts=counts[,.(geographicAreaM49, measuredItemChildCPC ,measuredItemParentCPC)]
data=data[counts, ,on=c("geographicAreaM49", "measuredItemChildCPC", "measuredItemParentCPC")]


data=imputeVariable(data,processingShareParamenters )
return(data)

}

