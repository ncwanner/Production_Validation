


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
        SETTINGS = ReadSettings("modules/animal_stockFRANCESCA/sws.yml")
        
        ## If you're not on the system, your settings will overwrite any others
        R_SWS_SHARE_PATH = SETTINGS[["share"]]
        
        ## Define where your certificates are stored
        SetClientFiles(SETTINGS[["certdir"]])
        
        ## Get session information from SWS. Token must be obtained from web interface
        GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                           token = SETTINGS[["token"]])
        
    }
sessionKey = swsContext.datasets[[1]]
oldData=FALSE

batchNumber=105


load("C:/Users/Rosa/Favorites/Github/sws_project/faoswsProduction/ProcessedSubmoduleSupportFiles/zeroWeight.RData")

#load("C:/Users/Rosa/Favorites/Github/sws_project/faoswsProduction/ProcessedSubmoduleSupportFiles/primaryInvolvedDescendents.RData")

completeImputationKey=getCompleteImputationKey("production")

completeImputationKey@dimensions$timePointYears@keys=c("2000","2001","2002",
                                                       "2003","2004","2005",
                                                       "2006","2007","2008",
                                                       "2009","2010","2011",
                                                       "2012","2013", "2014", "2015")



completeImputationKey@dimensions$geographicAreaM49@keys=c("454", "686", "1248", "716","360", "392", "484")

##-------------------------------------------------------------------------------------------------------------------------------------
## Get default 
params = defaultProcessedItemParams()
##-------------------------------------------------------------------------------------------------------------------------------------

processedCPC=ReadDatatable("processed_item")[,measured_item_cpc]
secondLoop=ReadDatatable("processed_item")[multiple_level==TRUE,measured_item_cpc]

##

#oil=c("2168", "2167","21631.01", "2162", "2165", "2161", "21641.01", "21631.02","2166")
#milk=c("22249.01", "22241.01", "22254", "22253", "22110.02", "22251.01", "22251.02", "22120", "22241.02", "22212",
#       "22222.01", "22211", "22221.01", "22130.02", "22230.04", "22222.02", "22242.02", "22130.03", "22221.02", "22242.01", "22252",
#       "22230.01", "22249.02")
##-------------------------------------------------------------------------------------------------------------------------------------

##  At the moment I am loading the new tree (made by Cristina) that I have locally stored in a local subfolder
##  tree= load("C:\\Users\\Rosa\\Favorites\\Github\\sws_project\\faoswsProduction\\ProcessedSubmoduleSupportFiles\\tree13112017.RData")
##  I have internlly modified the getTree function in order to pull the tree from my local folder instead of from the SWS:

##tree=getTree()
 
## I am using the last tree Cristina sent me (the one containing updated shares for Japan as well)

##load("C:\\Users\\Rosa\\Favorites\\Github\\sws_project\\faoswsProduction\\ProcessedSubmoduleSupportFiles\\treeTest6SharesAllComm22-11.RData") 
load("C:\\Users\\Rosa\\Favorites\\Github\\sws_project\\faoswsProduction\\ProcessedSubmoduleSupportFiles\\treeTestAllSharesJapancopied.RData") 

#tree=tree[!is.na(extractionRate)]
#tree[share=="NaN", share:=NA_real_]

levels=findProcessingLevel(tree,"measuredItemParentCPC","measuredItemChildCPC")
setnames(levels, "temp","measuredItemParentCPC")
tree=merge(tree, levels, by="measuredItemParentCPC", all.x=TRUE)

    
setnames(tree,"timePointYearsSP","timePointYears")

## Select all the commodity involved (what I need is just the dependence parent-child,
##all the possibilities, no country-year specific)

treeRestricted=tree[,.(measuredItemParentCPC,measuredItemChildCPC,processingLevel)]
treeRestricted=treeRestricted[with(treeRestricted, order(measuredItemChildCPC))]
treeRestricted=treeRestricted[!duplicated(treeRestricted)]

## Get all the primary starting from the processed item (stored in the data.table: processed_item )
primaryInvolved=getPrimary(processedCPC, treeRestricted, params)

## Get all the primary children (all levels)
primaryInvolvedDescendents=getChildren( commodityTree = treeRestricted,
                                        parentColname ="measuredItemParentCPC",
                                        childColname = "measuredItemChildCPC",
                                        topNodes =primaryInvolved )
##-------------------------------------------------------------------------------------------------------------------------------------
# Get SUA data: all components stored in the SWS

##  if(oldData){
##      load("C:/Users/Rosa/Favorites/Github/sws_project/faoswsProduction/ProcessedSubmoduleSupportFiles/dataoldSua.RData")
##      data=data[measuredElementSuaFbs!="foodManufacturing", ]
##  }else{
##      load(file.path("C:/Users/Rosa/Favorites/Github/sws_project/StandardizationFiles/localFile","dataTEST6_v02.RData"))
##  }

##  # Get SUA data from PRODUCTION
##  start.time <- Sys.time()
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
##  itemKeys = primaryInvolvedDescendents[primaryInvolvedDescendents!="2351f"]
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
##   
##   
##   end.time <- Sys.time()
##   
##   time.taken <- end.time - start.time
##------------------------------------------------------------------------------------------------------------

   # Get SUA data from QA
   
  
   
   start.time <- Sys.time()
   ##areaKeys = GetCodeList(domain = "suafbs", dataset = "sua", "geographicAreaM49")
   areaKeys = completeImputationKey@dimensions$geographicAreaM49@keys
   timeKeys = completeImputationKey@dimensions$timePointYears@keys
   elemKeys = GetCodeTree(domain = "suafbs", dataset = "sua_unbalanced", "measuredElementSuaFbs")
   
   #    code              description
   # 1:   51                   Output
   # 2:   61              Inflow (Qt)
   # 3:   71 Variation Intial Exstenc
   # 4:   91             Outflow (Qt)
   # 5:  101     Use For Animals (Qt)
   # 6:  111     Use For Same Product
   # 7:  121                   Losses
   # 8:  131 Reemployment Same Sector  (remove it)
   
   fs_elements <- c("51", "61", "71", "91", "101", "111", "121", "131")
   
   elemKeys = elemKeys[parent %in% fs_elements,
                       paste0(children, collapse = ", ")]
   
   # code                  description
   # 1: 5141                     Food [t]
   # 2: 5164 Tourist consumption [1000 t]
   # 3: 5165     Industrial uses [1000 t]
   
   
   sws_elements <- c("5141", "5164", "5165")
   
   elemKeys = c(strsplit(elemKeys, ", ")[[1]], sws_elements)
   ##itemKeys = GetCodeList(domain = "suafbs", dataset = "sua", "measuredItemFbsSua")
   itemKeys = primaryInvolvedDescendents
   
   key = DatasetKey(domain = "suafbs", dataset = "sua_unbalanced", dimensions = list(
       geographicAreaM49 = Dimension(name = "geographicAreaM49", keys = areaKeys),
       measuredElementSuaFbs = Dimension(name = "measuredElementSuaFbs", keys = elemKeys),
       measuredItemFbsSua = Dimension(name = "measuredItemFbsSua", keys = itemKeys),
       timePointYears = Dimension(name = "timePointYears", keys = timeKeys)
   ))
   
   message("Reading SUA data...")
   
   ## This gets the values for all countries, all elements which are children of the
   ## element classes listed above, all CPCs in suafbs and all years between those
   ## specified by the user.
   
   ##!! 3 warnings about things that need to be changed !!#
   data = elementCodesToNames(data = GetData(key), itemCol = "measuredItemFbsSua",
                              elementCol = "measuredElementSuaFbs")
   
   
   
   data[measuredElementSuaFbs=="stock",measuredElementSuaFbs:="stockChange"]
   
   
   
   ## Add all the missing PRODUCTION row: if the production of a derived product does not exist it even if it is created by this 
   ## routine cannot be stored in the SUA table and consequently all the commodities that belongs to its descendents are not estimates
   ## or are estimated using only the TRADE and neglecting an important part of the supply components.
   
   prod=data[measuredElementSuaFbs=="production", .( geographicAreaM49, timePointYears ,measuredItemFbsSua)]
   ##all=unique(data[ ,.( geographicAreaM49, timePointYears ,measuredItemParentCPC)])
   
   ##------------------------------------------------------------
   ## all time point years
   timePointYears=key@dimensions$timePointYears@keys
   ## all countries
   geographicAreaM49=key@dimensions$geographicAreaM49@keys
   
   ##I have to build table containing the complete data key ( I have called it: all) 
   all1=merge(timePointYears, primaryInvolvedDescendents)
   setnames(all1, c("x","y"),c("timePointYears","measuredItemFbsSua"))
   
   
   all2=merge(geographicAreaM49, primaryInvolvedDescendents)
   setnames(all2, c("x","y"),c("geographicAreaM49","measuredItemFbsSua"))
   all1=data.table(all1)
   all2=data.table(all2)
   
   all=merge(all1, all2, by="measuredItemFbsSua", allow.cartesian = TRUE)
   ## the object all contais a complete key (all countries, all years, all items) for the production element
   ## To avoid duplicates I select just those row that are not already included into the SUA table
   noProd=setdiff(all,prod)
   ##I add the value and flags columns:
   noProd[,":="(c("measuredElementSuaFbs","Value","flagObservationStatus","flagMethod"), list("production",NA_real_,"M","u"))]
   
   ## I add this empty rows into the SUA table
   data=rbind(data,noProd)
   ##------------------------------------------------------------
   
 
   ##setnames(data, "measuredItemFbsSua", "measuredItemFbsSua")
  
   end.time <- Sys.time()
   time.taken <- end.time - start.time
   
   
   
   ##dataOLD=dataOLD[timePointYears %in% c("2000","2001","2002","2003","2004","2005","2006","2007","2008","2009"),]
   
   ##data=rbind(dataOLD, data)
   
##-------------------------------------------------------------------------------------------------------------------------------------
## Processed data (OUTPUT). I pull the data from the aproduction domain. Baasically I am pulling the already existing 
## production data,
   
## Restrict the complete imputation keys in ordert to pull only PRODUCTION:
completeImputationKey@dimensions$measuredElement@keys=c("5510")
completeImputationKey@dimensions$measuredItemCPC@keys=primaryInvolvedDescendents

dataProcessed=GetData(completeImputationKey)
dataProcessed[,timePointYears:=as.numeric(timePointYears)]
dataProcessed=dataProcessed[,oldFAOSTATdata:=Value]

##Artificially protect production data between the time window 2000-2009.
dataProcessed[timePointYears<2010,flagObservationStatus:="E"] 
dataProcessed[timePointYears<2010,flagMethod:="h"] 

dataProcessed=expandYear(dataProcessed,newYear=2016)

dataProcessed=removeInvalidFlag(dataProcessed, "Value", "flagObservationStatus", "flagMethod", normalised = TRUE)
dataProcessed=removeNonProtectedFlag(dataProcessed, "Value", "flagObservationStatus", "flagMethod", normalised = TRUE)





setnames(dataProcessed, "measuredItemCPC", "measuredItemChildCPC")
##At the moment we have SUA data for the time range 2000-2015 (I pulled data from QA)
dataProcessed=dataProcessed[timePointYears %in% c(2000:2015)]
##     ##-------------------------------------------------------------------------------------------------------------------------------------
##     ## data:SUA 
##     data=data[measuredItemSuaFbs %in% primaryInvolvedDescendents]
##     data=data[!is.na(measuredElementSuaFbs),]
##     
##     ##change the name of the ITEM columns: parents
##     setnames(data,"measuredItemSuaFbs","measuredItemParentCPC")
     
     ##-----------------------------------------------------------------------------------------------------------------------
     ## data:SUA (if I pulled data from SWS (the name of the ITEM column is different: measuredItemFbsSua))
     
     ## I keep only the item theat I need: processed Items and all those items in them trees
     data=data[measuredItemFbsSua %in% primaryInvolvedDescendents]
     data=data[!is.na(measuredElementSuaFbs),]
     
     ##change the name of the ITEM columns: parents
     setnames(data,"measuredItemFbsSua","measuredItemParentCPC")


##-----------------------------------------------------------------------------------------------------------------------
#Experiments of subsets of COUNTRIES: 

## I am currently using those 7 countries for the FBS experiment:
tree=tree[geographicAreaM49 %in% c("454", "686",  "1248", "716","360", "392", "484")]
##-----------------------------------------------------------------------------------------------------------------------

levels=unique(tree[, processingLevel])
allCountries=unique(tree[, geographicAreaM49])
data[,timePointYears:=as.numeric(timePointYears)]

allLevels=list()
start.time <- Sys.time()
for(lev in (seq(levels)-1))  {
    
    ##Loop by level
    treeCurrentLevel=tree[processingLevel==lev]
    finalByCountry=list()
    
    
    
    for(geo in   seq_along(allCountries)){
        
    ##Loop by country    
    currentGeo=allCountries[geo]
    ##Subset the data (SUA)    
    currentData=data[geographicAreaM49==currentGeo]
    ##Tresform TIME in a numeric variabible:
    currentData[,timePointYears:=as.numeric(timePointYears)]
    treeCurrentLevel[,timePointYears:=as.numeric(timePointYears)]
    
    ## Subset PRODUCTION data
    currentDataProcessed=dataProcessed[geographicAreaM49==currentGeo]
    currentDataProcessed[,timePointYears:=as.numeric(timePointYears)]
   
    ## To compute the PRODUCTION I need to evaluate how much (which percentage) of 
    ## the parent commodity availability is allocated in the productive process associate
    ## to a specific child item.
        
    ## Calculate share down up. Please note that currentData contains the SUA table.
    dataMergeTree=calculateShareDownUp(data=currentData,tree=treeCurrentLevel,
                                       params=params, printNegativeAvailability=TRUE)
    
    ## Here I merge the SUA table (already merged with tree), with the PRODUCTION DATA
    ## This merge produces many NA because currentDataProcessed contains all the derived item (and not just
    ## those at the level of the loop)
    inputOutputdata= merge(dataMergeTree,currentDataProcessed, by=c("geographicAreaM49","measuredItemChildCPC","timePointYears"),all.y=TRUE) 
 
    
    
    inputOutputdata[shareDownUp=="NaN",shareDownUp:=0]
    
    
    ##test the tree coming from Cristina and the use of old PROCESSING SHARES:
#    load("C:\\Users\\Rosa\\Favorites\\Github\\sws_project\\faoswsProduction\\ProcessedSubmoduleSupportFiles\\processingTreeTest6AllComm.RData") 
#    setnames(pTree, "timePointYearsSP", "timePointYears")
#    pTree[, timePointYears:=as.numeric(timePointYears)]
    
   
    inputOutputdata=calculateProcessingShare(inputOutputdata, printSharesGraterThan1=TRUE, param=params)

    ##-------------------------------------------------------------------------------------------------------------------------------------    
    
    ##inputOutputdata[, meanProcessingShare:=mean(processingShare, na.rm = TRUE), by=c("geographicAreaM49","measuredItemChildCPC","measuredItemParentCPC")]
    
    ##inputOutputdata[is.na(processingShare)&!is.na(meanProcessingShare),processingShare:=meanProcessingShare ]
    inputOutputdata[, newImputation:=availability*processingShare*extractionRate]
    
    finalByCountry[[geo]]=inputOutputdata
    
    ## Update production in data in order to add the just computed production
    ## at each loop we compute production for the following level, this prodution
    ## should be used in the following loop to compute the availabilities
    
    updateData=inputOutputdata[,.(geographicAreaM49, timePointYears, measuredItemChildCPC, newImputation)]
    updateData[, newImputation:=sum(newImputation,na.rm = TRUE), by=c("geographicAreaM49", "timePointYears", "measuredItemChildCPC")]
    updateData=unique(updateData)
    
    
    ##I change the column names bacause the commodities that now are children will be parent in the next loop
    setnames(updateData,"measuredItemChildCPC","measuredItemParentCPC")
    
    ##I merge the new results into the SUA table
    data=merge(data,updateData, by=c("geographicAreaM49", "timePointYears", "measuredItemParentCPC"), all.x=TRUE)
    ## Olnly non-protected production figures have to be overwritten:

    if(!oldData){
        data[,flagComb:=paste(flagObservationStatus,flagMethod,sep=";")]   
    
    flagValidTable=copy(flagValidTable)
    flagValidTable=flagValidTable[Protected==TRUE,]
    protected=flagValidTable[,protectedComb:=paste(flagObservationStatus,flagMethod,sep=";")]
    protected=protected[,protectedComb]
    
    
    data[geographicAreaM49==currentGeo & !(flagComb %in% protected) & measuredElementSuaFbs=="production" & !is.na(newImputation),
         ":="(c("Value","flagObservationStatus","flagMethod"),list(newImputation,"I","e"))]
    
    ##filter=!is.na(data[,newImputation]) & data[,measuredElementSuaFbs=="production"] & data[,flagComb] %in% protected
    
#   data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !flagComb %in% protected & !is.na(newImputation), Value:=newImputation]     
#   data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !flagComb %in% protected & !is.na(newImputation), flagObservationStatus:="I"]
#   data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !flagComb %in% protected & !is.na(newImputation), flagMethod:="e"]
    }
    
    if(oldData){
   data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(newImputation)& Protected==FALSE, Value:=newImputation]     
   data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(newImputation)& Protected==FALSE, flagObservationStatus:="I"]
   data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(newImputation)& Protected==FALSE, flagMethod:="e"]
    
        
       # data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(newImputation) , Value:=newImputation]     
       # data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(newImputation) , flagObservationStatus:="I"]
       # data[geographicAreaM49==currentGeo & measuredElementSuaFbs=="production" & !is.na(newImputation) , flagMethod:="e"]
        }
    
    
    data[,newImputation:=NULL]
    
    if(!oldData){
    data[,flagComb:=NULL]
    }
}

    allLevels[[lev+1]]=rbindlist(finalByCountry)
    
}


end.time <- Sys.time()
#-------------------------------------------------------------------------------------------------------------------------------------    
#-------------------------------------------------------------------------------------------------------------------------------------    


output=rbindlist(allLevels)
output=output[,.(geographicAreaM49,measuredItemChildCPC,timePointYears,
                 measuredItemParentCPC,availability,processingShare,Value,
                 flagObservationStatus,flagMethod,oldFAOSTATdata,newImputation)]

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
write.csv(toPlot, paste0("C:\\Users\\Rosa\\Desktop\\ProcessedCommodities\\BatchExpandedItems\\Batch",batchNumber,"\\toPlot",batchNumber,".csv"), row.names=FALSE)


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

## I comment the filter for the Save operation: not just the processe
##imputed=imputed[measuredItemCPC %in% processedCPC]
##The first save back should exclude those commodities imputed in the second round
imputed=imputed[!measuredItemCPC %in% secondLoop]

SaveData(domain = sessionKey@domain,
         dataset = sessionKey@dataset,
         data =  imputed)

##-------------------------------------------------------------------------------------------------------------------------------------    
##-------------------------------------------------------------------------------------------------------------------------------------    
##-------------------------------------------------------------------------------------------------------------------------------------    
##-------------------------------------------------------------------------------------------------------------------------------------   
##All the commodities flagged as "secondLoop"

## I keep from the tree just those line where the ITEMS are classified as "second loop" 
## 

parent=unique(tree[measuredItemChildCPC %in% secondLoop, measuredItemParentCPC])
tree=tree[measuredItemChildCPC %in% secondLoop]
tree=tree[measuredItemParentCPC %in% parent]
tree[,processingLevel:=NULL]

## now we have just one level and it is necessary to overwrite the old processingLvels
levels=findProcessingLevel(tree,"measuredItemParentCPC","measuredItemChildCPC")
setnames(levels, "temp","measuredItemParentCPC")
tree=merge(tree, levels, by="measuredItemParentCPC", all.x=TRUE)

allCountries=unique(tree[, geographicAreaM49])
data[,timePointYears:=as.numeric(timePointYears)]

##-------------------------------------------------------------------------------------------------------------------------------------   
completeImputationKey@dimensions$measuredElement@keys=c("5510")
completeImputationKey@dimensions$measuredItemCPC@keys=secondLoop

dataProcessedsecondLoop=GetData(completeImputationKey)

dataProcessedsecondLoop[,timePointYears:=as.numeric(timePointYears)]
dataProcessedsecondLoop=expandYear(dataProcessedsecondLoop,newYear=2016)
dataProcessedsecondLoop=dataProcessedsecondLoop[,oldFAOSTATdata:=Value]

##Some values have been artificially protected in order to activate the imputation process
dataProcessedsecondLoop[timePointYears %in% c("1991","1996","2001", "2006","2011"),
                        ":="(c("flagObservationStatus","flagMethod"), list("E","h"))]

dataProcessedsecondLoop=removeInvalidFlag(dataProcessedsecondLoop, "Value", "flagObservationStatus", "flagMethod", normalised = TRUE)
dataProcessedsecondLoop=removeNonProtectedFlag(dataProcessedsecondLoop, "Value", "flagObservationStatus", "flagMethod", normalised = TRUE)

setnames(dataProcessedsecondLoop, "measuredItemCPC", "measuredItemChildCPC")



##At the moment we have SUA data for the time range 2000-2015

dataProcessedsecondLoop=dataProcessedsecondLoop[timePointYears %in% c(2000:2015)]
##-------------------------------------------------------------------------------------------------------------------------------------   
tree=tree[geographicAreaM49 %in% c("454", "686", "1248", "716","360", "392", "484")]

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
    final=calculateProcessingShare(final,printSharesGraterThan1=TRUE, param = params)
    ##-------------------------------------------------------------------------------------------------------------------------------------    
    
    ##final[, meanProcessingShare:=mean(processingShare, na.rm = TRUE), by=c("geographicAreaM49","measuredItemChildCPC","measuredItemParentCPC")]
    
    ##final[is.na(processingShare)&!is.na(meanProcessingShare),processingShare:=meanProcessingShare ]
    final[, newImputation:=availability*processingShare*extractionRate]
    
    finalByCountrysecondLoop[[geo]]=final
}
allLevelssecondLoop=rbindlist(finalByCountrysecondLoop)
##-------------------------------------------------------------------------------------------------------------------------------------    
##-------------------------------------------------------------------------------------------------------------------------------------   



outputSecondLoop=allLevelssecondLoop[,.(geographicAreaM49,measuredItemChildCPC,timePointYears,measuredItemParentCPC,availability,processingShare,Value,flagObservationStatus,flagMethod,oldFAOSTATdata,newImputation)]

## This passage is to sum up the production of a derived commodities coming from more than one parent
finalOutputSecondLoop = outputSecondLoop[,  list(newImputation = sum(newImputation, na.rm = TRUE)),
                     by =c ("geographicAreaM49","measuredItemChildCPC","timePointYears")]
#-------------------------------------------------------------------------------------------------------------------------------------    

## merge OldFAOSTATdata with 
orig=outputSecondLoop[,.(geographicAreaM49,measuredItemChildCPC,timePointYears,Value,oldFAOSTATdata,flagObservationStatus,flagMethod)]

orig=orig[!duplicated(orig)]

imputedSecondLoop=merge(finalOutputSecondLoop,orig, by=c("geographicAreaM49", "measuredItemChildCPC","timePointYears"), allow.cartesian = TRUE)

imputedSecondLoop[flagObservationStatus=="M" & flagMethod=="u" & !is.na(newImputation), ":="(c("Value", "flagObservationStatus", "flagMethod"), list(newImputation,"I","e"))]



imputedSecondLoop[,flagComb:=paste(flagObservationStatus,flagMethod,sep=";")]   
imputedSecondLoop[, PROTECTED:=FALSE]
imputedSecondLoop[flagComb %in% protected, PROTECTED:=TRUE]
imputedSecondLoop[PROTECTED==TRUE,newImputation:=oldFAOSTATdata]
toPlotSecondLoop=imputedSecondLoop

write.csv(toPlotSecondLoop, paste0("C:\\Users\\Rosa\\Desktop\\ProcessedCommodities\\BatchExpandedItems\\Batch",batchNumber,"\\toPlotSecondLoop",batchNumber,".csv"), row.names=FALSE)


## Save back second loop
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

##The first save back should exclude those commodities imputed in the second round


SaveData(domain = sessionKey@domain,
         dataset = sessionKey@dataset,
         data =  imputedSecondLoop)

message("Imputation Completed Successfully")