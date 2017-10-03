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
sessionKey = swsContext.datasets[[1]]
oldData=TRUE

load("C:/Users/Rosa/Favorites/Github/sws_project/faoswsProduction/ProcessedSubmoduleSupportFiles/zeroWeight.RData")

#load("C:/Users/Rosa/Favorites/Github/sws_project/faoswsProduction/ProcessedSubmoduleSupportFiles/primaryInvolvedDescendents.RData")

completeImputationKey=getCompleteImputationKey("production")

completeImputationKey@dimensions$timePointYears@keys=c("2000","2001","2002",
                                                       "2003","2004","2005",
                                                       "2006","2007","2008",
                                                       "2009","2010","2011",
                                                       "2012","2013")

##-------------------------------------------------------------------------------------------------------------------------------------


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
params$level = "processingLevel"
params$availVar = "availability"
params$shareDownUp="shareDownUp"
params$processingShare="processingShare"



##-------------------------------------------------------------------------------------------------------------------------------------

processedCPC=ReadDatatable("processed_item")[,measured_item_cpc]
secondLoop=ReadDatatable("processed_item")[multiple_level==TRUE,measured_item_cpc]
##-------------------------------------------------------------------------------------------------------------------------------------
tree=getTree()
setnames(tree,"timePointYearsSP","timePointYears")


## Select all the commodity involved
treeRestricted=tree[,.(measuredItemParentCPC,measuredItemChildCPC,processingLevel)]
treeRestricted=treeRestricted[with(treeRestricted, order(measuredItemChildCPC))]
treeRestricted=treeRestricted[!duplicated(treeRestricted)]
primaryInvolved=getPrimary(processedCPC, treeRestricted, params)
primaryInvolvedDescendents=getChildren( commodityTree = treeRestricted,
                                        parentColname ="measuredItemParentCPC",
                                        childColname = "measuredItemChildCPC",
                                        topNodes =primaryInvolved )
##-------------------------------------------------------------------------------------------------------------------------------------
# Get SUA data

#load(file.path("C:/Users/Rosa/Favorites/Github/sws_project/StandardizationFiles/localFile", "data_AllTradeFAOSTAT.RData"))

if(oldData){
    load("C:/Users/Rosa/Favorites/Github/sws_project/faoswsProduction/ProcessedSubmoduleSupportFiles/dataoldSua.RData")
    data=data[measuredElementSuaFbs!="foodManufacturing", ]
}else{
    load(file.path("C:/Users/Rosa/Favorites/Github/sws_project/StandardizationFiles/localFile", "data_AllTradeFAOSTAT.RData"))
}



##  ##areaKeys = GetCodeList(domain = "suafbs", dataset = "sua", "geographicAreaM49")
##  areaKeys = completeImputationKey@dimensions$geographicAreaM49@keys
##  timeKeys = completeImputationKey@dimensions$timePointYears@keys
##  elemKeys = GetCodeTree(domain = "suafbs", dataset = "sua", "measuredElementSuaFbs")
##  
##  #    code              description
##  # 1:   51                   Output
##  # 2:   61              Inflow (Qt)
##  # 3:   71 Variation Intial Exstenc
##  # 4:   91             Outflow (Qt)
##  # 5:  101     Use For Animals (Qt)
##  # 6:  111     Use For Same Product
##  # 7:  121                   Losses
##  # 8:  131 Reemployment Same Sector  (remove it)
##  
##  fs_elements <- c("51", "61", "71", "91", "101", "111", "121", "131")
##  
##  elemKeys = elemKeys[parent %in% fs_elements,
##                      paste0(children, collapse = ", ")]
##  
##  # code                  description
##  # 1: 5141                     Food [t]
##  # 2: 5164 Tourist consumption [1000 t]
##  # 3: 5165     Industrial uses [1000 t]
##  
##  
##  sws_elements <- c("5141", "5164", "5165")
##  
##  elemKeys = c(strsplit(elemKeys, ", ")[[1]], sws_elements)
##  ##itemKeys = GetCodeList(domain = "suafbs", dataset = "sua", "measuredItemSuaFbs")
##  itemKeys = primaryInvolvedDescendents
##  
##  key = DatasetKey(domain = "suafbs", dataset = "sua_unbalanced", dimensions = list(
##      geographicAreaM49 = Dimension(name = "geographicAreaM49", keys = areaKeys),
##      measuredElementSuaFbs = Dimension(name = "measuredElementSuaFbs", keys = elemKeys),
##      measuredItemSuaFbs = Dimension(name = "measuredItemSuaFbs", keys = itemKeys),
##      timePointYears = Dimension(name = "timePointYears", keys = timeKeys)
##  ))
##  
##  message("Reading SUA data...")
##  
##  ## This gets the values for all countries, all elements which are children of the
##  ## element classes listed above, all CPCs in suafbs and all years between those
##  ## specified by the user.
##
##  ##!! 3 warnings about things that need to be changed !!#
##   data = elementCodesToNames(data = GetData(key), itemCol = "measuredItemSuaFbs",
##   elementCol = "measuredElementSuaFbs")
##   setnames(data, "measuredItemSuaFbs", "measuredItemSuaFbs")
##-------------------------------------------------------------------------------------------------------------------------------------


## Processed data (OUTPUT)

completeImputationKey@dimensions$measuredElement@keys=c("5510")
completeImputationKey@dimensions$measuredItemCPC@keys=primaryInvolvedDescendents


dataProcessed=GetData(completeImputationKey)
dataProcessed=expandYear(dataProcessed)
dataProcessed=dataProcessed[,oldFAOSTATdata:=Value]
dataProcessed[timePointYears %in% c("1991","1996","2001", "2006","2011"),
              ":="(c("flagObservationStatus","flagMethod"), list("E","h"))]

dataProcessed=removeInvalidFlag(dataProcessed, "Value", "flagObservationStatus", "flagMethod", normalised = TRUE)
dataProcessed=removeNonProtectedFlag(dataProcessed, "Value", "flagObservationStatus", "flagMethod", normalised = TRUE)

setnames(dataProcessed, "measuredItemCPC", "measuredItemChildCPC")



##At the moment we have SUA data for the time range 2000-2013

dataProcessed=dataProcessed[timePointYears %in% c(2000:2013)]
##-------------------------------------------------------------------------------------------------------------------------------------


data=data[measuredItemSuaFbs %in% primaryInvolvedDescendents]
data=data[!is.na(measuredElementSuaFbs),]
setnames(data,"measuredItemSuaFbs","measuredItemParentCPC")


#tree=tree[geographicAreaM49 %in% c("380", "764", "276","840", "484", "686","800", "152","704" )]
tree=tree[geographicAreaM49 %in% c("368", "288","716","384","32" )]



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
    ##Subset the data    
    currentData=data[geographicAreaM49==currentGeo]
    currentData[,timePointYears:=as.numeric(timePointYears)]
    treeCurrentLevel[,timePointYears:=as.numeric(timePointYears)]
    currentDataProcessed=dataProcessed[geographicAreaM49==currentGeo]
    currentDataProcessed[,timePointYears:=as.numeric(timePointYears)]
   
    ## Calculate share
    dataMergeTree=calculateShareDownUp(data=currentData,tree=treeCurrentLevel,
                                       params=params, printNegativeAvailability=TRUE)
    
    
    inputOutputdata= merge(dataMergeTree,currentDataProcessed, by=c("geographicAreaM49","measuredItemChildCPC","timePointYears"),all.y=TRUE) 
 
    inputOutputdata=calculateProcessingShare(inputOutputdata, printSharesGraterThan1=TRUE)
    ##-------------------------------------------------------------------------------------------------------------------------------------    
    
    ##inputOutputdata[, meanProcessingShare:=mean(processingShare, na.rm = TRUE), by=c("geographicAreaM49","measuredItemChildCPC","measuredItemParentCPC")]
    
    ##inputOutputdata[is.na(processingShare)&!is.na(meanProcessingShare),processingShare:=meanProcessingShare ]
    inputOutputdata[, newImputation:=availability*processingShare*extractionRate]
    
    finalByCountry[[geo]]=inputOutputdata
    
    ##update production in data in order to add the just computed production
    ##at each loop we compute production for the following level, this prodution
    ## should be used in the following loop to compute the availabilities
    
    updateData=inputOutputdata[,.(geographicAreaM49, timePointYears, measuredItemChildCPC, newImputation)]
    updateData[, newImputation:=sum(newImputation,na.rm = TRUE), by=c("geographicAreaM49", "timePointYears", "measuredItemChildCPC")]
    updateData=unique(updateData)
    ##I change the column names bacause the commodities that now are children will be parent in the next loop
    setnames(updateData,"measuredItemChildCPC","measuredItemParentCPC")

    
    data=merge(data,updateData, by=c("geographicAreaM49", "timePointYears", "measuredItemParentCPC"), all.x=TRUE)
    
    ## Olnly non-protected production figures have to be overwritten:
    if(!oldData){
    data[,flagComb:=paste(flagObservationStatus,flagMethod,sep=";")]   
    
    flagValidTable=copy(flagValidTable)
    flagValidTable=flagValidTable[Protected==TRUE,]
    protected=flagValidTable[,protectedComb:=paste(flagObservationStatus,flagMethod,sep=";")]
    protected=protected[,protectedComb]
    
    
    data[geographicAreaM49==currentGeo & !(flagComb %in% protected) & measuredElementSuaFbs=="production", ":="(c("Value","flagObservationStatus","flagMethod"),list("NA","M","u"))]
    
    ##filter=!is.na(data[,newImputation]) & data[,measuredElementSuaFbs=="production"] & data[,flagComb] %in% protected
    
    data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !flagComb %in% protected & !is.na(newImputation), Value:=newImputation]     
    data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !flagComb %in% protected & !is.na(newImputation), flagObservationStatus:="I"]
    data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !flagComb %in% protected & !is.na(newImputation), flagMethod:="e"]
    }
    
    if(oldData){
   #data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(newImputation)& Protected==FALSE, Value:=newImputation]     
   #data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(newImputation)& Protected==FALSE, flagObservationStatus:="I"]
   #data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(newImputation)& Protected==FALSE, flagMethod:="e"]
    
        
        data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(newImputation) , Value:=newImputation]     
        data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(newImputation) , flagObservationStatus:="I"]
        data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(newImputation) , flagMethod:="e"]
    
    
        
        }
    
    
    data[,newImputation:=NULL]
    
    if(!oldData){
    data[,flagComb:=NULL]
    }
}

    allLevels[[lev+1]]=rbindlist(finalByCountry)
    
}


##-------------------------------------------------------------------------------------------------------------------------------------    

##Beer, margarine and tallow
parent=unique(tree[measuredItemChildCPC %in% secondLoop, measuredItemParentCPC])
tree=tree[measuredItemChildCPC %in% secondLoop]
tree=tree[measuredItemParentCPC %in% parent]
tree[,processingLevel:=NULL]

## now we have just one level and it is necessary to overwrite che old processingLvels
levels=findProcessingLevel(tree,"measuredItemParentCPC","measuredItemChildCPC")
setnames(levels, "temp","measuredItemParentCPC")
tree=merge(tree, levels, by="measuredItemParentCPC", all.x=TRUE)

allCountries=unique(tree[, geographicAreaM49])
data[,timePointYears:=as.numeric(timePointYears)]

##-------------------------------------------------------------------------------------------------------------------------------------   
completeImputationKey@dimensions$measuredElement@keys=c("5510")
completeImputationKey@dimensions$measuredItemCPC@keys=secondLoop

dataProcessedsecondLoop=GetData(completeImputationKey)
dataProcessedsecondLoop=expandYear(dataProcessedsecondLoop)
dataProcessedsecondLoop=dataProcessedsecondLoop[,oldFAOSTATdata:=Value]

##Some values have been artificially protected in order to activate the imputation process
dataProcessedsecondLoop[timePointYears %in% c("1991","1996","2001", "2006","2011"),
              ":="(c("flagObservationStatus","flagMethod"), list("E","h"))]

dataProcessedsecondLoop=removeInvalidFlag(dataProcessedsecondLoop, "Value", "flagObservationStatus", "flagMethod", normalised = TRUE)
dataProcessedsecondLoop=removeNonProtectedFlag(dataProcessedsecondLoop, "Value", "flagObservationStatus", "flagMethod", normalised = TRUE)

setnames(dataProcessedsecondLoop, "measuredItemCPC", "measuredItemChildCPC")



##At the moment we have SUA data for the time range 2000-2013

dataProcessedsecondLoop=dataProcessedsecondLoop[timePointYears %in% c(2000:2013)]
##-------------------------------------------------------------------------------------------------------------------------------------   
##tree=tree[geographicAreaM49 %in% c("380", "764", "484")]

levels=unique(tree[, processingLevel])
allCountries=unique(tree[, geographicAreaM49])

data[,timePointYears:=as.numeric(timePointYears)]


treeCurrentLevel=tree[processingLevel==0]
##setnames(treeCurrentLevel,"timePointYearsSP","timePointYears")
    
finalByCountrysecondLoop=list()
    for(geo in   seq_along(allCountries)){
        currentGeo=allCountries[geo]
        
        currentData=data[geographicAreaM49==currentGeo]
        currentData[,timePointYears:=as.numeric(timePointYears)]
        treeCurrentLevel[,timePointYears:=as.numeric(timePointYears)]
        currentDataProcessed=dataProcessed[geographicAreaM49==currentGeo]
        currentDataProcessed[,timePointYears:=as.numeric(timePointYears)]
        
        
        
        
        
        dataMergeTree=calculateShareDownUp(data=currentData,tree=treeCurrentLevel, params=params,printNegativeAvailability=TRUE)
        final= merge(dataMergeTree,currentDataProcessed, by=c("geographicAreaM49","measuredItemChildCPC","timePointYears"),all.y=TRUE) 
        final=calculateProcessingShare(final,printSharesGraterThan1=TRUE)
        ##-------------------------------------------------------------------------------------------------------------------------------------    
        
        ##final[, meanProcessingShare:=mean(processingShare, na.rm = TRUE), by=c("geographicAreaM49","measuredItemChildCPC","measuredItemParentCPC")]
        
        ##final[is.na(processingShare)&!is.na(meanProcessingShare),processingShare:=meanProcessingShare ]
        final[, newImputation:=availability*processingShare*extractionRate]
        
        finalByCountrysecondLoop[[geo]]=final
          
##       ##update production in data in order to add the just computed production
##       ##at each loop we compute production for the following level, this prodution
##       ## should be used in the following loop to compute the availabilities
##       
##       updateData=final[,.(geographicAreaM49, timePointYears, measuredItemChildCPC, newImputation)]
##       updateData[, newImputation:=sum(newImputation,na.rm = TRUE), by=c("geographicAreaM49", "timePointYears", "measuredItemChildCPC")]
##       updateData=unique(updateData)
##       ##I change the column names bacause the commodities that now are children will be parent in the next loop
##       setnames(updateData,"measuredItemChildCPC","measuredItemParentCPC")
##       
##       
##       data=merge(data,updateData, by=c("geographicAreaM49", "timePointYears", "measuredItemParentCPC"), all.x=TRUE)
##       
##       ## Olnly non-protected production figures have to be overwritten:
##       
##       data[,flagComb:=paste(flagObservationStatus,flagMethod,sep=";")]   
##       
##       flagValidTable=copy(flagValidTable)
##       flagValidTable=flagValidTable[Protected==TRUE,]
##       protected=flagValidTable[,protectedComb:=paste(flagObservationStatus,flagMethod,sep=";")]
##       protected=protected[,protectedComb]
##       
##       
##       data[geographicAreaM49==currentGeo & !(flagComb %in% protected) & measuredElementSuaFbs=="production", ":="(c("Value","flagObservationStatus","flagMethod"),list("NA","M","u"))]
##       
##       ##filter=!is.na(data[,newImputation]) & data[,measuredElementSuaFbs=="production"] & data[,flagComb] %in% protected
##       
##       data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !flagComb %in% protected & !is.na(newImputation), Value:=newImputation]     
##       data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !flagComb %in% protected & !is.na(newImputation), flagObservationStatus:="I"]
##       data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !flagComb %in% protected & !is.na(newImputation), flagMethod:="e"]
##       
##       
##       if(oldData){
##           data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(newImputation)& Protected==FALSE, Value:=newImputation]     
##           data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(newImputation)& Protected==FALSE, flagObservationStatus:="I"]
##           data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(newImputation)& Protected==FALSE, flagMethod:="e"]
##       }
##       
##       
##       data[,newImputation:=NULL]
##       data[,flagComb:=NULL]
    }
    
        allLevelssecondLoop=rbindlist(finalByCountrysecondLoop)
    




##-------------------------------------------------------------------------------------------------------------------------------------    





output=rbindlist(allLevels)
output=output[,.(geographicAreaM49,measuredItemChildCPC,timePointYears,measuredItemParentCPC,availability,processingShare,Value,flagObservationStatus,flagMethod,oldFAOSTATdata,newImputation)]

## This passage is to sum up the production of a derived commodities coming from more than one parent
finalOutput = output[,  list(newImputation = sum(newImputation, na.rm = TRUE)),
                        by =c ("geographicAreaM49","measuredItemChildCPC","timePointYears")]
#-------------------------------------------------------------------------------------------------------------------------------------    

## merge OldFAOSTATdata with 
orig=output[,.(geographicAreaM49,measuredItemChildCPC,timePointYears,Value,oldFAOSTATdata,flagObservationStatus,flagMethod)]

orig=orig[!duplicated(orig)]

imputed=merge(finalOutput,orig, by=c("geographicAreaM49", "measuredItemChildCPC","timePointYears"), allow.cartesian = TRUE)

imputed[flagObservationStatus=="M" & flagMethod=="u" & !is.na(newImputation), ":="(c("Value", "flagObservationStatus", "flagMethod"), list(newImputation,"I","e"))]



imputed[,flagComb:=paste(flagObservationStatus,flagMethod,sep=";")]   
imputed[, PROTECTED:=FALSE]
imputed[flagComb %in% protected, PROTECTED:=TRUE]
imputed[PROTECTED==TRUE,newImputation:=oldFAOSTATdata]
toPlot=imputed



## Save back
imputed[,newImputation:=NULL]
imputed[,oldFAOSTATdata:=NULL]
imputed[,flagComb:=NULL]
imputed[,PROTECTED:=NULL]

imputed[,measuredElement:="5510"]
setnames(imputed, "measuredItemChildCPC", "measuredItemCPC")

imputed= removeInvalidDates(data = imputed, context = sessionKey)
imputed= postProcessing(data =  imputed) 
imputed=imputed[flagObservationStatus=="I" & flagMethod=="e"]

imputed=imputed[,.(measuredElement,geographicAreaM49, measuredItemCPC,
                   timePointYears,Value,flagObservationStatus,flagMethod)]
##Save back only those commodities that
imputed=imputed[measuredItemCPC %in% processedCPC]

##The first save back should exclude those commodities imputed in the second round
imputed=imputed[!measuredItemCPC %in% secondLoop]

SaveData(domain = sessionKey@domain,
         dataset = sessionKey@dataset,
         data =  imputed)




allLevelssecondLoop
finalOutputSecondLoop=allLevelssecondLoopoutput[,.(geographicAreaM49,measuredItemChildCPC,timePointYears,measuredItemParentCPC,availability,processingShare,Value,flagObservationStatus,flagMethod,oldFAOSTATdata,newImputation)]

origsecondLoop=allLevelssecondLoop[,.(geographicAreaM49,measuredItemChildCPC,timePointYears,Value,oldFAOSTATdata,flagObservationStatus,flagMethod)]

origsecondLoop=origsecondLoop[!duplicated(origsecondLoop)]



finalOutputSecondLoop = finalOutputSecondLoop[,  list(newImputation = sum(newImputation, na.rm = TRUE)),
       by =c ("geographicAreaM49","measuredItemChildCPC","timePointYears")]

imputedSecondLoop=merge(finalOutputSecondLoop,origsecondLoop, by=c("geographicAreaM49", "measuredItemChildCPC","timePointYears"), allow.cartesian = TRUE)


imputedSecondLoop[flagObservationStatus=="M" & flagMethod=="u" & !is.na(newImputation), ":="(c("Value", "flagObservationStatus", "flagMethod"), list(newImputation,"I","e"))]


imputedSecondLoop[,flagComb:=paste(flagObservationStatus,flagMethod,sep=";")]   
imputedSecondLoop[, PROTECTED:=FALSE]
imputedSecondLoop[flagComb %in% protected, PROTECTED:=TRUE]
imputedSecondLoop[PROTECTED==TRUE,newImputation:=oldFAOSTATdata]

toPlotSecondLoop=imputedSecondLoop

## Save back
imputedSecondLoop[,newImputation:=NULL]
imputedSecondLoop[,oldFAOSTATdata:=NULL]
imputedSecondLoop[,flagComb:=NULL]
imputedSecondLoop[,PROTECTED:=NULL]

imputedSecondLoop[,measuredElement:="5510"]
setnames(imputedSecondLoop, "measuredItemChildCPC", "measuredItemCPC")

imputedSecondLoop= removeInvalidDates(data = imputedSecondLoop, context = sessionKey)
imputedSecondLoop= postProcessing(data =  imputedSecondLoop) 
imputedSecondLoop=imputedSecondLoop[flagObservationStatus=="I" & flagMethod=="e"]

imputedSecondLoop=imputedSecondLoop[,.(measuredElement,geographicAreaM49, measuredItemCPC,
                   timePointYears,Value,flagObservationStatus,flagMethod)]
##Save back only those commodities that
imputedSecondLoop=imputedSecondLoop[measuredItemCPC %in% secondLoop]

SaveData(domain = sessionKey@domain,
         dataset = sessionKey@dataset,
         data =  imputedSecondLoop)
