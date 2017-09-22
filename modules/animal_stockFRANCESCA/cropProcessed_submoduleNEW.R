suppressMessages({
    library(data.table)
    library(faosws)
    library(faoswsFlag)
    library(faoswsUtil)
    library(faoswsImputation)
    library(faoswsProduction)
    library(faoswsProcessing)
    library(faoswsEnsure)
    library(magrittr)
    library(dplyr)
    library(sendmailR)
    library(faoswsStandardization)
    
})




if(CheckDebug()){
    
    library(faoswsModules)
    SETTINGS = ReadSettings("sws.yml")
    
    ## If you're not on the system, your settings will overwrite any others
    R_SWS_SHARE_PATH = SETTINGS[["share"]]
    
    ## Define where your certificates are stored
    SetClientFiles(SETTINGS[["certdir"]])
    
    ## Get session information from SWS. Token must be obtained from web interface
    GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                       token = SETTINGS[["token"]])
    
}

oldData=FALSE

load("C:/Users/Rosa/Favorites/Github/sws_project/faoswsProduction/ProcessedSubmoduleSupportFiles/zeroWeight.RData")
completeImputationKey=getCompleteImputationKey()

completeImputationKey@dimensions$timePointYears@keys=c("2000","2001","2002",
                                                       "2003","2004","2005",
                                                       "2006","2007","2008",
                                                       "2009","2010","2011",
                                                       "2012","2013")

tree=getTree()
setnames(tree,"timePointYearsSP","timePointYears")

params = defaultStandardizationParameters()
params$itemVar = "measuredItemSuaFbs"
params$mergeKey[params$mergeKey == "measuredItemCPC"] = "measuredItemSuaFbs"
params$elementVar = "measuredElementSuaFbs"
params$childVar = "measuredItemChildCPC"
params$parentVar = "measuredItemParentCPC"
params$productionCode = "production"
params$importCode = "imports"
params$exportCode = "exports"
params$stockCode = "stockChange"
params$foodCode = "food"
params$feedCode = "feed"
params$seedCode = "seed"
params$wasteCode = "loss"
params$industrialCode = "industrial"
params$touristCode = "tourist"
params$foodProcCode = "foodManufacturing"
params$residualCode = "residual"
params$createIntermetiateFile= "TRUE"
params$protected = "Protected"
params$official = "Official"



processedCPC=c("22249.01", "22241.01" ,"22254"  ,  "22253"  ,  "22110.02", "26110",    "22251.01" ,"22251.02" ,"21521" ,   "22120"  ,  "22241.02",
                     "22212"  ,  "22222.01", "22211" ,   "22221.01" ,"21523"  ,  "22130.02" ,"22230.04", "22222.02" ,"22242.02", "22130.03", "22221.02",
                     "22242.01", "22252"  ,  "22230.01", "22249.02","01921.02" ,"0143"   ,  "23540"   , "2168" ,    "21691.12" ,"2167",
                     "21691.07" ,"21631.01" ,"2351f"  ,  "24310.01", "21700.02","2162",     "2165" ,    "2161"    , "24212.02" ,"21641.01" ,
                     "21631.02" ,"2166"   ,  "21691.14" ,"01491.02" ,"21691.02")


##Time consuming operation:
##uniqueLevels=unique(tree[,.(geographicAreaM49, timePointYears)])
##primaryInvolved=list()
##for(i in seq_len(nrow(uniqueLevels))){
##    
##    
##    filter = uniqueLevels[i, ]
##    treeSubset=tree[filter,,on = c("geographicAreaM49", "timePointYears")]    
##    primaryInvolved[[i]]=getPrimary(processedCPC, treeSubset, params)
##}
##
##primaryInvolvedList=unique(unlist(primaryInvolved))
##
##
##primaryInvolvedDescendents=list()
##
##for(i in seq_len(length(primaryInvolvedList)))
##{ 
##    primaryInvolvedDescendents[[i]]=data.table(getChildren( commodityTree = tree,
##                                                            parentColname ="measuredItemParentCPC",
##                                                            childColname = "measuredItemChildCPC",
##                                                            topNodes =primaryInvolvedList[i] ))
##}

##primaryInvolvedDescendents= rbindlist(primaryInvolvedDescendents)
#### I need a vector of all the primary descendents:
##primaryInvolvedDescendents=primaryInvolvedDescendents$V1






##-------------------------------------------------------------------------------------------------------------------------------------

## Processed data (OUTPUT)

completeImputationKey@dimensions$measuredElement@keys=c("5510")
completeImputationKey@dimensions$measuredItemCPC@keys=primaryInvolvedDescendents


dataProcessed=GetData(completeImputationKey)
dataProcessed=expandYear(dataProcessed)
dataProcessed=dataProcessed[,SWSdata:=Value]
dataProcessed[timePointYears %in% c("1991","1996","2001", "2006","2011"),
              ":="(c("flagObservationStatus","flagMethod"), list("E","h"))]

dataProcessed=removeInvalidFlag(dataProcessed, "Value", "flagObservationStatus", "flagMethod", normalised = TRUE)
dataProcessed=removeNonProtectedFlag(dataProcessed, "Value", "flagObservationStatus", "flagMethod", normalised = TRUE)

setnames(dataProcessed, "measuredItemCPC", "measuredItemChildCPC")



##At the moment we have SUA data for the time range 2000-2013

dataProcessed=dataProcessed[timePointYears %in% c(2000:2013)]
##-------------------------------------------------------------------------------------------------------------------------------------
load(file.path("C:/Users/Rosa/Favorites/Github/sws_project/StandardizationFiles/localFile", "data_AllTradeFAOSTAT.RData"))

##if(oldData){
##
##load("C:/Users/Rosa/Favorites/Github/sws_project/faoswsProduction/ProcessedSubmoduleSupportFiles/dataoldSua.RData")
##data=data[measuredElementSuaFbs!="foodManufacturing", ]
##}


data=data[measuredItemSuaFbs %in% primaryInvolvedDescendents]
data=data[!is.na(measuredElementSuaFbs),]
setnames(data,"measuredItemSuaFbs","measuredItemParentCPC")
##pilotCountries=c("380", "764", "276","840", "484", "686","800", "152","704" )

tree=tree[geographicAreaM49 %in% c("380", "764", "276","840", "484", "686","800", "152","704" )]

##tree=tree[geographicAreaM49 %in% c("116")]

##tree=tree[geographicAreaM49 %in% c("320","724","188","222","191" )]
##tree=tree[geographicAreaM49 %in% c("360" )]
##

levels=unique(tree[, processingLevel])
allCountries=unique(tree[, geographicAreaM49])
data[,timePointYears:=as.numeric(timePointYears)]



allLevels=list()

for(lev in (seq(levels)-1))  {
    
    
    treeCurrentLevel=tree[processingLevel==lev]
    ##setnames(treeCurrentLevel,"timePointYearsSP","timePointYears")
    
    finalByCountry=list()
    
    
    for(geo in   seq_along(allCountries)){
        currentGeo=allCountries[geo]
        
    currentData=data[geographicAreaM49==currentGeo]
    currentData[,timePointYears:=as.numeric(timePointYears)]
    treeCurrentLevel[,timePointYears:=as.numeric(timePointYears)]
    currentDataProcessed=dataProcessed[geographicAreaM49==currentGeo]
    currentDataProcessed[,timePointYears:=as.numeric(timePointYears)]
    ##treeCurrentLevel=treeCurrentLevel[geographicAreaM49==currentGeo]
    
    
    dataMergeTree=merge(currentData,treeCurrentLevel, by=c("measuredItemParentCPC", "geographicAreaM49","timePointYears"))
                        ##allow.cartesian = TRUE)
    
    
    
    ##Simple availability that we interpret as FOOD PROCESSING
    dataMergeTree[, availability := sum(ifelse(is.na(Value), 0, Value) *
                                        ifelse(measuredElementSuaFbs == "production", 1,
                                        ifelse(measuredElementSuaFbs == "imports", 1,
                                        ifelse(measuredElementSuaFbs == "exports" , -1, 
                                        ifelse(measuredElementSuaFbs == "stockChange", -1,
                                        ifelse(measuredElementSuaFbs == "food", -1,
                                        ifelse(measuredElementSuaFbs == "feed" , -1,
                                        ifelse(measuredElementSuaFbs == "loss", -1,
                                        ifelse(measuredElementSuaFbs == "seed", -1,
                                        ifelse(measuredElementSuaFbs == "industrial", -1,
                                        ifelse(measuredElementSuaFbs == "tourist", -1, 0))))))))))),
                  by = c("geographicAreaM49","timePointYears","measuredItemParentCPC","measuredItemChildCPC")]
    
    
    ##-------------------------------------------------------------------------------------------------------
    ##Deviate the negative agailability to be manually checked
    nagativeAvailability=dataMergeTree[availability<1]
    nagativeAvailability=nagativeAvailability[,.(measuredItemParentCPC,	geographicAreaM49, availability,timePointYears)]
    nagativeAvailability=unique(nagativeAvailability)
    directory= "C:/Users/Rosa/Desktop/DERIVATIVES/negativeAvailability/"
    dir.create(paste0(directory, lev), recursive=TRUE)
    write.csv(nagativeAvailability, paste0(directory,lev, "/",currentGeo, "nagativeAvailability",".csv"), sep=";",row.names = F)
    
    
    ##------------------------------------------------------------------------------------------------------  
    
    
    
    
    dataMergeTree=dataMergeTree[,.(measuredItemParentCPC, geographicAreaM49, timePointYears ,  
                                          measuredItemChildCPC ,extractionRate,processingLevel, availability)] 
    dataMergeTree = dataMergeTree[,
                      list(availability = mean(availability, na.rm = TRUE)),
                      by = c("measuredItemParentCPC","geographicAreaM49","timePointYears","measuredItemChildCPC","extractionRate","processingLevel")]
    
    
    
    dataMergeTree[, weight:=1]
    dataMergeTree[measuredItemChildCPC %in% zeroWeight, weight:=0]
    
   
    
    dataMergeTree[availability<1,availability:=0]				
    
    dataMergeTree[,availabilitieChildEquivalent:=availability*extractionRate]
    dataMergeTree[, sumAvail:=sum(availabilitieChildEquivalent), by=c("measuredItemChildCPC","timePointYears","geographicAreaM49")]
    
    dataMergeTree[,shareDownUp:=NA]
    
    dataMergeTree[,shareDownUp:=availabilitieChildEquivalent/sumAvail]
    ##currentDataProcessed=currentDataProcessed[flagObservationStatus!="M" & flagMethod!="-",]
    
    final= merge(dataMergeTree,currentDataProcessed, by=c("geographicAreaM49","measuredItemChildCPC","timePointYears"),all.y=TRUE) 
    final=final[!is.na(measuredItemParentCPC)]
    
    final[, processingShare:=(((Value/extractionRate)*shareDownUp)/availability)]
    final[,weight:=NULL]
    
    
    ##-------------------------------------------------------------------------------------------------------
    ##Deviate the negative agailability to be manually checked
    processingShareGraterThan1=final[processingShare>1]
    directory= "C:/Users/Rosa/Desktop/DERIVATIVES/unfeasibleProcessingshare/"
    dir.create(paste0(directory, lev), recursive=TRUE)
    write.csv(processingShareGraterThan1, paste0(directory,lev, "/",currentGeo, "processingShareGraterThan1",".csv"), sep=";",row.names = F)
    
    
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
   
    ##finalInpute=copy(final)
    final[,processingShareFlagObservationStatus:="M"]
    final[,processingShareFlagMethod:="u"]
    
    
    final[!is.na(processingShare),processingShareFlagObservationStatus:="T"]
    final[!is.na(processingShare),processingShareFlagMethod:="-"]
    
    
    counts = final[,
                             sum(!is.na(processingShare)),
                             by = c(processingShareParamenters$byKey)]
    counts[V1!=0]
    counts=counts[V1!=0]
    counts=counts[,.(geographicAreaM49, measuredItemChildCPC ,measuredItemParentCPC)]
    final=final[counts, ,on=c("geographicAreaM49", "measuredItemChildCPC", "measuredItemParentCPC")]
    
    
    final=imputeVariable(final,processingShareParamenters )
    
    ##-------------------------------------------------------------------------------------------------------------------------------------    
    
    ##final[, meanProcessingShare:=mean(processingShare, na.rm = TRUE), by=c("geographicAreaM49","measuredItemChildCPC","measuredItemParentCPC")]
    
    ##final[is.na(processingShare)&!is.na(meanProcessingShare),processingShare:=meanProcessingShare ]
    final[, output:=availability*processingShare*extractionRate]
    
    finalByCountry[[geo]]=final
    
    ##update production in data in order to add the just computed production
    ##at each loop we compute production for the following level, this prodution
    ## should be used in the following loop to compute the availabilities
    
    updateData=final[,.(geographicAreaM49, timePointYears, measuredItemChildCPC, output)]
    updateData[, output:=sum(output,na.rm = TRUE), by=c("geographicAreaM49", "timePointYears", "measuredItemChildCPC")]
    updateData=unique(updateData)
    ##I change the label bacause the commodities that now are children will be parent in the next loop
    setnames(updateData,"measuredItemChildCPC","measuredItemParentCPC")

    
    data=merge(data,updateData, by=c("geographicAreaM49", "timePointYears", "measuredItemParentCPC"), all.x=TRUE)
    
    ## Olnly non-protected 
    data[,flagComb:=paste(flagObservationStatus,flagMethod,sep=";")]   
    
    flagValidTable=copy(flagValidTable)
    flagValidTable=flagValidTable[Protected==TRUE,]
    protected=flagValidTable[,protectedComb:=paste(flagObservationStatus,flagMethod,sep=";")]
    protected=protected[,protectedComb]
    
    
    data[geographicAreaM49==currentGeo & !(flagComb %in% protected) & measuredElementSuaFbs=="production", ":="(c("Value","flagObservationStatus","flagMethod"),list("NA","M","u"))]
    
    ##filter=!is.na(data[,output]) & data[,measuredElementSuaFbs=="production"] & data[,flagComb] %in% protected
    
    data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !flagComb %in% protected & !is.na(output), Value:=output]     
    data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !flagComb %in% protected & !is.na(output), flagObservationStatus:="I"]
    data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !flagComb %in% protected & !is.na(output), flagMethod:="e"]
    
    
    if(oldData){
    data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(output)& Protected==FALSE, Value:=output]     
    data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(output)& Protected==FALSE, flagObservationStatus:="I"]
    data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(output)& Protected==FALSE, flagMethod:="e"]
    }
    
    
    data[,output:=NULL]
    data[,flagComb:=NULL]
}

    allLevels[[lev+1]]=rbindlist(finalByCountry)
    
}


test=rbindlist(allLevels)
test=test[,.(geographicAreaM49,measuredItemChildCPC,timePointYears,measuredItemParentCPC,availability,processingShare,Value,flagObservationStatus,flagMethod,SWSdata,output)]

orig=test[,.(geographicAreaM49,measuredItemChildCPC,timePointYears,SWSdata,flagObservationStatus,flagMethod)]

orig=orig[!duplicated(orig)]



testFinal = test[,  list(output = sum(output, na.rm = TRUE)),
       by =c ("geographicAreaM49","measuredItemChildCPC","timePointYears")]

tt=merge(testFinal,orig, by=c("geographicAreaM49", "measuredItemChildCPC","timePointYears"), allow.cartesian = TRUE)
tt[,flagComb:=paste(flagObservationStatus,flagMethod,sep=";")]   
tt[, PROTECTED:=FALSE]
tt[flagComb %in% protected, PROTECTED:=TRUE]
tt[PROTECTED==TRUE,output:=SWSdata]


ITALY=tt
