


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
        
        batchNumber=120
        dir.create(paste0("C:\\Users\\Rosa\\Desktop\\ProcessedCommodities\\BatchExpandedItems\\Batch", batchNumber), recursive=TRUE)
        
    }
sessionKey = swsContext.datasets[[1]]
oldData=FALSE





processed47=c("22249.01", "22241.01" ,"22254"  ,  "22253"  ,  "22110.02", "26110",    "22251.01" ,"22251.02" ,"21521" ,   "22120"  ,  "22241.02",
              "22212"  ,  "22222.01", "22211" ,   "22221.01" ,"21523"  ,  "22130.02" ,"22230.04", "22222.02" ,"22242.02", "22130.03", "22221.02",
              "22242.01", "22252"  ,  "22230.01", "22249.02","01921.02" ,"0143"   ,  "23540"   , "2168" ,    "21691.12" ,"2167",
              "21691.07" ,"21631.01" ,"2351f"  ,  "24310.01", "21700.02","2162",     "2165" ,    "2161"    , "24212.02" ,"21641.01" ,
              "21631.02" ,"2166"   ,  "21691.14" ,"01491.02" ,"21691.02")


#load("C:/Users/Rosa/Favorites/Github/sws_project/faoswsProduction/ProcessedSubmoduleSupportFiles/zeroWeightLast.RData")

#load("C:/Users/Rosa/Favorites/Github/sws_project/faoswsProduction/ProcessedSubmoduleSupportFiles/primaryInvolvedDescendents.RData")

completeImputationKey=getCompleteImputationKey("production")

completeImputationKey@dimensions$timePointYears@keys=c("2000","2001","2002",
                                                       "2003","2004","2005",
                                                       "2006","2007","2008",
                                                       "2009","2010","2011",
                                                       "2012","2013", "2014", "2015")



##completeImputationKey@dimensions$geographicAreaM49@keys=c("454", "686", "1248", "716","360", "392", "484")

##-------------------------------------------------------------------------------------------------------------------------------------
## Get default 
params = defaultProcessedItemParams()
##-------------------------------------------------------------------------------------------------------------------------------------

processedCPC=ReadDatatable("processed_item")[,measured_item_cpc]
##secondLoop=ReadDatatable("processed_item")[multiple_level==TRUE,measured_item_cpc]

##processed47=c("22249.01", "22241.01" ,"22254"  ,  "22253"  ,  "22110.02", "26110",    "22251.01" ,"22251.02" ,"21521" ,   "22120"  ,  "22241.02",
##              "22212"  ,  "22222.01", "22211" ,   "22221.01" ,"21523"  ,  "22130.02" ,"22230.04", "22222.02" ,"22242.02", "22130.03", "22221.02",
##              "22242.01", "22252"  ,  "22230.01", "22249.02","01921.02" ,"0143"   ,  "23540"   , "2168" ,    "21691.12" ,"2167",
##              "21691.07" ,"21631.01" ,"2351f"  ,  "24310.01", "21700.02","2162",     "2165" ,    "2161"    , "24212.02" ,"21641.01" ,
##              "21631.02" ,"2166"   ,  "21691.14" ,"01491.02" ,"21691.02")
##


##processedCPC=processed47
##-------------------------------------------------------------------------------------------------------------------------------------

##  At the moment I am loading the new tree (made by Cristina) that I have locally stored in a local subfolder
##  tree= load("C:\\Users\\Rosa\\Favorites\\Github\\sws_project\\faoswsProduction\\ProcessedSubmoduleSupportFiles\\tree13112017.RData")
##  I have internlly modified the getTree function in order to pull the tree from my local folder instead of from the SWS:

##tree=getTree()
 
## I am using the last tree Cristina sent me (the one containing updated shares for Japan as well)

##load("C:\\Users\\Rosa\\Favorites\\Github\\sws_project\\faoswsProduction\\ProcessedSubmoduleSupportFiles\\treeTest6SharesAllComm22-11.RData") 
##load("C:\\Users\\Rosa\\Favorites\\Github\\sws_project\\faoswsProduction\\ProcessedSubmoduleSupportFiles\\treeTestAllSharesJapancopied.RData") 
##load("C:\\Users\\Rosa\\Favorites\\Github\\sws_project\\faoswsProduction\\ProcessedSubmoduleSupportFiles\\treeFromIO_japanCopied_basicMod_120617.RData") 
##treeFromIO_AllCountries_ErShare_up15_basicChanges.RData
##tree=fread("C:\\Users\\Rosa\\Favorites\\Github\\sws_project\\faoswsProduction\\ProcessedSubmoduleSupportFiles\\fullTree2export.csv") 
##tree=fread("C:\\Users\\Rosa\\Favorites\\Github\\sws_project\\faoswsProduction\\ProcessedSubmoduleSupportFiles\\fullTree2exportTEST_Francesca.csv") 


##areaKeys=GetCodeList(domain = "suafbs", dataset = "ess_fbs_commodity_tree", "geographicAreaM49")[,code]
geoImputationSelection = swsContext.computationParams$imputation_country_selection
sessionCountry=getQueryKey("geographicAreaM49", sessionKey)
selectedCountry =
    switch(geoImputationSelection,
           "session" = sessionCountry,
           "all" = completeImputationKey@dimensions$geographicAreaM49@keys)

startYear=swsContext.computationParams$startYear
imputationStartYear=swsContext.computationParams$startImputation
endYear=swsContext.computationParams$endYear

areaKeys=selectedCountry
timeKeys=as.character(c(startYear:endYear))
itemKeysParent=GetCodeList(domain = "suafbs", dataset = "ess_fbs_commodity_tree", "measuredItemParentCPC")[,code]
itemKeysChild=GetCodeList(domain = "suafbs", dataset = "ess_fbs_commodity_tree", "measuredItemChildCPC")[,code]
##The only element I need from the tree is the "extraction rate"
elemKeys=c("5423")

keyTree = DatasetKey(domain = "suafbs", dataset = "ess_fbs_commodity_tree", dimensions = list(
    geographicAreaM49 = Dimension(name = "geographicAreaM49", keys = areaKeys),
    measuredElementSuaFbs = Dimension(name = "measuredElementSuaFbs", keys = elemKeys),
    measuredItemParentCPC = Dimension(name = "measuredItemParentCPC", keys = itemKeysParent),
    measuredItemChildCPC = Dimension(name = "measuredItemChildCPC", keys = itemKeysChild),
    timePointYears = Dimension(name = "timePointYears", keys = timeKeys)
))




##load("C:\\Users\\Rosa\\Favorites\\Github\\sws_project\\faoswsProduction\\ProcessedSubmoduleSupportFiles\\fullTree2export_v02nocuts.RData")
##tree=tree2export
#tree=tree[timePointYears>1999]


tree=GetData(keyTree)
setnames(tree, "Value", "extractionRate")
tree[,flag_obs_status_v2:=NULL ]
tree[,flagMethod:=NULL ]
tree[,measuredElementSuaFbs:=NULL ]

tree=tree[!is.na(extractionRate)]

#tree=tree[!is.na(extractionRate)]
#tree[share=="NaN", share:=NA_real_]
uniqueLevels = tree[, .N, by = c("geographicAreaM49", "timePointYears")]
uniqueLevels[, N := NULL]
levels=list()
treeLevels=list()
for (i in seq_len(nrow(uniqueLevels))) {
filter=uniqueLevels[i, ]
treeCurrent=tree[filter, , on = c("geographicAreaM49", "timePointYears")]
levels=findProcessingLevel(treeCurrent,"measuredItemParentCPC","measuredItemChildCPC")
setnames(levels, "temp","measuredItemParentCPC")
treeLevels[[i]]= merge(treeCurrent, levels, by=c("measuredItemParentCPC"), all.x=TRUE)
}

tree=rbindlist(treeLevels)


##setnames(levels, "temp","measuredItemParentCPC")
##tree=merge(tree, levels, by="measuredItemParentCPC", all.x=TRUE)

    
##setnames(tree,"timePointYearsSP","timePointYears")

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




## find the so-called secondLoop commodities:
## select all the unique combination of country-commodity-level:
multipleLevels=unique(tree[,.( geographicAreaM49, measuredItemChildCPC, processingLevel)])

## count all the repeated country-commodity combinations.
multipleLevels[,n:=.N, by=c("measuredItemChildCPC", "geographicAreaM49")]
## the n>1 it means that that commodity appears at least for one country in more that one level
## If this situation occurs just for some countries, it is not a problem, because at the end of the process we decide
## to keep only ONE production-contribution and in particular the contribution of the highest level in the tree 
## hierachy (if it is just one level, we are properly considering ALL the production components)
## see row 560 of this script.

secondLoop=unique(multipleLevels[n!=1,measuredItemChildCPC])

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
   areaKeys = tree[, unique(geographicAreaM49)]
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
   
   ## Note that all the columns of the all datatable have class=factor
   all[,measuredItemFbsSua:=as.character(measuredItemFbsSua)]
   all[,timePointYears:=as.character(timePointYears)]
   all[,geographicAreaM49:=as.character(geographicAreaM49)]
   
   
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
completeImputationKey@dimensions$geographicAreaM49@keys=areaKeys
completeImputationKey@dimensions$timePointYears@keys=timeKeys

dataProcessed=GetData(completeImputationKey)
dataProcessed[,timePointYears:=as.numeric(timePointYears)]
dataProcessed=dataProcessed[,oldFAOSTATdata:=Value]

##Artificially protect production data between the time window 2000-2009.
dataProcessed[timePointYears<imputationStartYear,flagObservationStatus:="E"] 
dataProcessed[timePointYears<imputationStartYear,flagMethod:="h"] 

dataProcessed=expandYear(dataProcessed,newYear=endYear)

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
##tree=tree[geographicAreaM49 %in% c("454", "686",  "1248", "716","360", "392", "484")]
##-----------------------------------------------------------------------------------------------------------------------

levels=unique(tree[, processingLevel])
allCountries=unique(tree[, geographicAreaM49])
data[,timePointYears:=as.numeric(timePointYears)]
tree[,timePointYears:=as.character(timePointYears)]
tree[,timePointYears:=as.numeric(timePointYears)]

allLevels=list()
start.time <- Sys.time()
for(lev in (seq(levels)-1))  {
   
    message("Processing level ", lev)
    
    ##Loop by level
    treeCurrentLevel=tree[processingLevel==lev]
    finalByCountry=list()
    
    
    
    for(geo in   seq_along(allCountries)){
        message("Processing country ", geo )
    ##Loop by country    
    currentGeo=allCountries[geo]
    ##Subset the data (SUA)    
    currentData=data[geographicAreaM49==currentGeo]
    ##Tresform TIME in a numeric variabible:
    currentData[,timePointYears:=as.numeric(timePointYears)]
    

    
    ## Subset PRODUCTION data
    currentDataProcessed=dataProcessed[geographicAreaM49==currentGeo]
    currentDataProcessed[,timePointYears:=as.numeric(timePointYears)]
   
    ## To compute the PRODUCTION I need to evaluate how much (which percentage) of 
    ## the parent commodity availability is allocated in the productive process associate
    ## to a specific child item.
        
    ## Calculate share down up. Please note that currentData contains the SUA table.
    dataMergeTree=calculateShareDownUp(data=currentData,tree=treeCurrentLevel,
                                       params=params, printNegativeAvailability=FALSE,
                                       batchNumber=batchNumber)
    
    ## Here I merge the SUA table (already merged with tree), with the PRODUCTION DATA
    ## This merge produces many NA because currentDataProcessed contains all the derived item (and not just
    ## those at the level of the loop)
    inputOutputdata= merge(dataMergeTree,currentDataProcessed, by=c("geographicAreaM49","measuredItemChildCPC","timePointYears"),all.y=TRUE) 
 
    
    
    inputOutputdata[shareDownUp=="NaN",shareDownUp:=0]
    
    
    ##test the tree coming from Cristina and the use of old PROCESSING SHARES:
#    load("C:\\Users\\Rosa\\Favorites\\Github\\sws_project\\faoswsProduction\\ProcessedSubmoduleSupportFiles\\processingTreeTest6AllComm.RData") 
#    setnames(pTree, "timePointYearsSP", "timePointYears")
#    pTree[, timePointYears:=as.numeric(timePointYears)]
    
   
    inputOutputdata=calculateProcessingShare(inputOutputdata, printSharesGraterThan1=FALSE, param=params, zeroWeightVector=zeroWeight)

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

## Those commodities that appear in more than one level of the commodity-tree hierachy are considered children 
## of the highest item in the hierachy.

## I have to expand the list of secondLoop commodities: I impose that all the descentents of a second-loop
## commodities are classified as "second-loop":

secondLoopChildren=getChildren( commodityTree = treeRestricted,
                                parentColname ="measuredItemParentCPC",
                                childColname = "measuredItemChildCPC",
                                topNodes =secondLoop )

secondLoop=secondLoopChildren

## This passage is to sum up the productions of a derived commodities coming from more
## than one parent (those commodities  are flagged as second-loop). To identify the highest 
## level of the hierarchy I have to choose the min value in the processingLevel column.

output[, minProcessingLevel:=NA_real_]

## The min of the processing-level is not only item-specific, but also country-time specific, since the 
## the commodity-tree may change over the time and the countries.

output[measuredItemChildCPC %in% secondLoop, minProcessingLevel:=min(processingLevel), by=c("geographicAreaM49","timePointYears", "measuredItemChildCPC")]

## I remove (just for those commodities classified as secondLoop) the production-contributions coming from parents 
## lower in the hierachy (it means with an higher processing-level)
output=output[is.na(minProcessingLevel) | processingLevel==minProcessingLevel]

#finalOutput = output[!(measuredItemChildCPC %in% secondLoop),  list(newImputation = sum(newImputation, na.rm = TRUE)),
#                        by =c ("geographicAreaM49","measuredItemChildCPC","timePointYears")]

#-------------------------------------------------------------------------------------------------------------------------------------    
#-------------------------------------------------------------------------------------------------------------------------------------    
## The following lines create the dataset useful for validation purposes:
## I want to sum the newImputation into the totNewImputation, but I want to keep
## all the constributions to double check the final result

if(CheckDebug()){
output[,  totNewImputation := sum(newImputation, na.rm = TRUE),
       by =c ("geographicAreaM49","measuredItemChildCPC","timePointYears")]

## outPutforValidation is the file created to validate the output from team B/C
outPutforValidation=copy(output)



outPutforValidation=outPutforValidation[,.(geographicAreaM49,measuredItemChildCPC, timePointYears, measuredItemParentCPC, extractionRate,
                                           processingLevel ,availability,shareDownUp,processingShare,newImputation, totNewImputation)]

directory=paste0("C:/Users/Rosa/Desktop/ProcessedCommodities/BatchExpandedItems/Batch",batchNumber,"/finalValidation")
    dir.create(directory)
fileForValidation2(outPutforValidation,   dir=directory)

  ## for validation purposes it is extremly important to produce validation files filtered for those 47
  ## commodies plut flours (that are upposed to be pubblished)
  
  flours=c("23110","23170","23120", "23120.02", "23120.07", "23170.01", "23120.90", "23120.08", "23170.04", "23120.03",
           "23120.05", "23120.10", "23170.03", "23120.01", "23170.02", "23120.04", "23120.06", "23120.09", "21920", "23999.02")
  
  toBePubblished=c(processed47, flours)
}
  
  #fileForValidationP = fileForValidation[measured %in% toBePubblished]
  #
  #directoryP=paste0("C:/Users/Rosa/Desktop/ProcessedCommodities/BatchExpandedItems/Batch",batchNumber,"/finalValidationOnlyItemsToPubblish")
  #fileForValidation(fileForValidationP, lastAvStandardizationBatch=120,  dir=directoryP)

#-------------------------------------------------------------------------------------------------------------------------------------    
#-------------------------------------------------------------------------------------------------------------------------------------    

## Here I fisically sum by child the newImputation (that now are disaggregated by parent-contribution)
finalOutput=output[,  list(newImputation = sum(newImputation, na.rm = TRUE)),
         by =c ("geographicAreaM49","measuredItemChildCPC","timePointYears")]

#output[,.(geographicAreaM49,measuredItemChildCPC,timePointYears,
#         measuredItemParentCPC,availability,processingShare,Value,
#         flagObservationStatus,flagMethod,oldFAOSTATdata,newImputation, processingLevel)]

#-------------------------------------------------------------------------------------------------------------------------------------    

## I subset the output datatable in order to make comparison between  oldFAOSTATdata and newImputations (I keep just the Value and oldFAOSTATdata columns)
orig=output[,.(geographicAreaM49,measuredItemChildCPC,timePointYears,Value,oldFAOSTATdata,flagObservationStatus,flagMethod)]
## This subset of output is full of duplicated rows because each child can have more than one parents, and new imputations have been preduced
## for each of its parent. Each new imputations represent the contribution of one single parent item to the production of the derived item.
orig=orig[!duplicated(orig)]

## finalOutput contains all the newImputation (olready aggregated with respect to all the parent contribuions)
imputed=merge(finalOutput,orig, by=c("geographicAreaM49", "measuredItemChildCPC","timePointYears"), allow.cartesian = TRUE)

## Only M,u figures are overwritten by the new imputations.
imputed[flagObservationStatus=="M" & flagMethod=="u" & !is.na(newImputation), ":="(c("Value", "flagObservationStatus", "flagMethod"), list(newImputation,"I","e"))]

#-------------------------------------------------------------------------------------------------------------------------------------    
## The column PROTECTED is created just to PLOT the output and distinguish between new imputations and protected figures.
imputed[,flagComb:=paste(flagObservationStatus,flagMethod,sep=";")]   
imputed[, PROTECTED:=FALSE]
imputed[flagComb %in% protected, PROTECTED:=TRUE]
imputed[PROTECTED==TRUE,newImputation:=oldFAOSTATdata]
toPlot=imputed
## This table is saved just to produce comparisond between batches: 

if(CheckDebug()){
write.csv(toPlot, paste0("C:\\Users\\Rosa\\Desktop\\ProcessedCommodities\\BatchExpandedItems\\Batch",batchNumber,"\\toPlot",batchNumber,".csv"), row.names=FALSE)
plotResult(toPlot, batchNumber,toBePubblished)
}

## ## This table is saved just to produce comparison between batches: 
## write.csv(toPlot, paste0("C:\\Users\\Rosa\\Desktop\\ProcessedCommodities\\BatchExpandedItems\\Batch",batchNumber,"\\toPlot",batchNumber,".csv"), row.names=FALSE)
## plotResult(toPlot, batchNumber, toBePubblished)
#-------------------------------------------------------------------------------------------------------------------------------------    


## Save back

## Remove the auxiliary columns created in the process
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


## I comment the filter for the Save operation: not just the processe
##imputed=imputed[measuredItemCPC %in% processedCPC]
##The first save back should exclude those commodities imputed in the second round
##imputed=imputed[!measuredItemCPC %in% secondLoop]

 SaveData(domain = sessionKey@domain,
          dataset = sessionKey@dataset,
          data =  imputed)

###         ##-------------------------------------------------------------------------------------------------------------------------------------    
###         ##-------------------------------------------------------------------------------------------------------------------------------------    
###         ##-------------------------------------------------------------------------------------------------------------------------------------    
###         ##-------------------------------------------------------------------------------------------------------------------------------------   
###         ##All the commodities flagged as "secondLoop"
###         
###         ## I keep from the tree just those line where the ITEMS are classified as "second loop" 
###         ## 
###         ## If I wan to run the module just for the famous 47 derived items that has to be pubblished, the second loop are those three.
###         ##secondLoop47=c("21700.02","21523","24310.01")
###         
###         
###         parent=unique(tree[measuredItemChildCPC %in% secondLoop, measuredItemParentCPC])
###         tree=tree[measuredItemChildCPC %in% secondLoop]
###         tree=tree[measuredItemParentCPC %in% parent]
###         tree[,processingLevel:=NULL]
###         
###         ##  When I have enlarged the set of derived items to be imputed, it might happen that a commodities 
###         ##  Which is in the secondLoop set is child of a commodity which is in the second loop set
###         ##  unique(tree[(measuredItemParentCPC %in% secondLoop),])
###         ##  measuredItemParentCPC geographicAreaM49 timePointYears measuredItemChildCPC extractionRate  share
###         ##  1:                 21523              1248           1961                F1243         1.0000 1.0000
###         ##  2:              21529.03               484           1961                21523         0.7998 0.0381
###         ##  3:              23210.05              1248           1981                24490         5.0000 1.0000
###         ##  4:              23220.04              1248           1961             23210.05         1.0000 1.0000
###         ##  5:              39130.02               716           1961             39130.04         1.0000 1.0000
###         ## 
###         ##  unique(tree[(measuredItemParentCPC %in% secondLoop),measuredItemChildCPC])
###         ## [1] "F1243"    "21529.03" "F1275"    "21523"    "24490"    "23670.01" "2413"     "23210.05" "23210.03" "24110"    "39130.04"
###         ## I delete this dependency because the purpose of this second loop is to approximate the commodity tree in order to transform 
###         ## the secondLoop set of item in a "first-level" items:
###         
###         tree=tree[!(measuredItemParentCPC %in% secondLoop),]
###         
###         ## now we have just one level and it is necessary to overwrite the old processingLvels
###         levels=findProcessingLevel(tree,"measuredItemParentCPC","measuredItemChildCPC")
###         setnames(levels, "temp","measuredItemParentCPC")
###         tree=merge(tree, levels, by="measuredItemParentCPC", all.x=TRUE)
###         
###         allCountries=unique(tree[, geographicAreaM49])
###         data[,timePointYears:=as.numeric(timePointYears)]
###         
###         ##-------------------------------------------------------------------------------------------------------------------------------------   
###         completeImputationKey@dimensions$measuredElement@keys=c("5510")
###         completeImputationKey@dimensions$measuredItemCPC@keys=secondLoop
###         
###         dataProcessedsecondLoop=GetData(completeImputationKey)
###         
###         dataProcessedsecondLoop[,timePointYears:=as.numeric(timePointYears)]
###         dataProcessedsecondLoop=expandYear(dataProcessedsecondLoop,newYear=2016)
###         dataProcessedsecondLoop=dataProcessedsecondLoop[,oldFAOSTATdata:=Value]
###         
###         ##Some values have been artificially protected in order to activate the imputation process
###         dataProcessedsecondLoop[timePointYears %in% c("1991","1996","2001", "2006","2011"),
###                                 ":="(c("flagObservationStatus","flagMethod"), list("E","h"))]
###         
###         dataProcessedsecondLoop=removeInvalidFlag(dataProcessedsecondLoop, "Value", "flagObservationStatus", "flagMethod", normalised = TRUE)
###         dataProcessedsecondLoop=removeNonProtectedFlag(dataProcessedsecondLoop, "Value", "flagObservationStatus", "flagMethod", normalised = TRUE)
###         
###         setnames(dataProcessedsecondLoop, "measuredItemCPC", "measuredItemChildCPC")
###         
###         
###         
###         ##At the moment we have SUA data for the time range 2000-2015
###         
###         dataProcessedsecondLoop=dataProcessedsecondLoop[timePointYears %in% c(2000:2015)]
###         ##-------------------------------------------------------------------------------------------------------------------------------------   
###         tree=tree[geographicAreaM49 %in% c("454", "686", "1248", "716","360", "392", "484")]
###         
###         levels=unique(tree[, processingLevel])
###         allCountries=unique(tree[, geographicAreaM49])
###         
###         data[,timePointYears:=as.numeric(timePointYears)]
###         
###         
###         treeCurrentLevel=tree[processingLevel==0]
###         
###         ##setnames(treeCurrentLevel,"timePointYearsSP","timePointYears")
###         
###         finalByCountrysecondLoop=list()
###         
###         lev=99
###         
###         
###         for(geo in   seq_along(allCountries)){
###             currentGeo=allCountries[geo]
###             
###             currentData=data[geographicAreaM49==currentGeo]
###             currentData[,timePointYears:=as.numeric(timePointYears)]
###             treeCurrentLevel[,timePointYears:=as.numeric(timePointYears)]
###             currentDataProcessed=dataProcessed[geographicAreaM49==currentGeo]
###             currentDataProcessed[,timePointYears:=as.numeric(timePointYears)]
###             
###             dataMergeTree=calculateShareDownUp(data=currentData,tree=treeCurrentLevel, params=params,printNegativeAvailability=TRUE, batchNumber=batchNumber)
###             final= merge(dataMergeTree,currentDataProcessed, by=c("geographicAreaM49","measuredItemChildCPC","timePointYears"),all.y=TRUE) 
###             final=calculateProcessingShare(final,printSharesGraterThan1=TRUE, param = params, zeroWeightVector=zeroWeight)
###             ##-------------------------------------------------------------------------------------------------------------------------------------    
###             
###             ##final[, meanProcessingShare:=mean(processingShare, na.rm = TRUE), by=c("geographicAreaM49","measuredItemChildCPC","measuredItemParentCPC")]
###             
###             ##final[is.na(processingShare)&!is.na(meanProcessingShare),processingShare:=meanProcessingShare ]
###             final[, newImputation:=availability*processingShare*extractionRate]
###             
###             finalByCountrysecondLoop[[geo]]=final
###         }
###         allLevelssecondLoop=rbindlist(finalByCountrysecondLoop)
###         ##-------------------------------------------------------------------------------------------------------------------------------------    
###         ##-------------------------------------------------------------------------------------------------------------------------------------   
###         outputToMergeSecondLoop=copy(allLevelssecondLoop)
###         
###         outputSecondLoop47=allLevelssecondLoop[measuredItemChildCPC %in% processed47]
###         ##I have to save in the batch folder the output for those 47 commodity to be validated:
###         write.csv(outputSecondLoop47, paste0("C:\\Users\\Rosa\\Desktop\\ProcessedCommodities\\BatchExpandedItems\\Batch",batchNumber,"\\outputSecondLoop47.csv"), row.names=F)
###         
###         outputSecondLoop=allLevelssecondLoop[,.(geographicAreaM49,measuredItemChildCPC,timePointYears,measuredItemParentCPC,availability,processingShare,Value,flagObservationStatus,flagMethod,oldFAOSTATdata,newImputation)]
###         
###         
###         ## This passage is to sum up the production of a derived commodities coming from more than one parent
###         finalOutputSecondLoop = outputSecondLoop[,  list(newImputation = sum(newImputation, na.rm = TRUE)),
###                              by =c ("geographicAreaM49","measuredItemChildCPC","timePointYears")]
###         #-------------------------------------------------------------------------------------------------------------------------------------    
###         
###         ## merge OldFAOSTATdata with 
###         orig=outputSecondLoop[,.(geographicAreaM49,measuredItemChildCPC,timePointYears,Value,oldFAOSTATdata,flagObservationStatus,flagMethod)]
###         
###         orig=orig[!duplicated(orig)]
###         
###         imputedSecondLoop=merge(finalOutputSecondLoop,orig, by=c("geographicAreaM49", "measuredItemChildCPC","timePointYears"), allow.cartesian = TRUE)
###         
###         imputedSecondLoop[flagObservationStatus=="M" & flagMethod=="u" & !is.na(newImputation), ":="(c("Value", "flagObservationStatus", "flagMethod"), list(newImputation,"I","e"))]
###         
###         
###         
###         imputedSecondLoop[,flagComb:=paste(flagObservationStatus,flagMethod,sep=";")]   
###         imputedSecondLoop[, PROTECTED:=FALSE]
###         imputedSecondLoop[flagComb %in% protected, PROTECTED:=TRUE]
###         imputedSecondLoop[PROTECTED==TRUE,newImputation:=oldFAOSTATdata]
###         toPlotSecondLoop=imputedSecondLoop
###         
###         write.csv(toPlotSecondLoop, paste0("C:\\Users\\Rosa\\Desktop\\ProcessedCommodities\\BatchExpandedItems\\Batch",batchNumber,"\\toPlotSecondLoop",batchNumber,".csv"), row.names=FALSE)
###         
###         
###         ## Save back second loop
###         imputedSecondLoop[,newImputation:=NULL]
###         imputedSecondLoop[,oldFAOSTATdata:=NULL]
###         imputedSecondLoop[,flagComb:=NULL]
###         imputedSecondLoop[,PROTECTED:=NULL]
###         
###         imputedSecondLoop[,measuredElement:="5510"]
###         setnames(imputedSecondLoop, "measuredItemChildCPC", "measuredItemCPC")
###         
###         imputedSecondLoop= removeInvalidDates(data = imputedSecondLoop, context = sessionKey)
###         imputedSecondLoop= postProcessing(data =  imputedSecondLoop) 
###         imputedSecondLoop=imputedSecondLoop[flagObservationStatus=="I" & flagMethod=="e"]
###         
###         imputedSecondLoop=imputedSecondLoop[,.(measuredElement,geographicAreaM49, measuredItemCPC,
###                            timePointYears,Value,flagObservationStatus,flagMethod)]
###         ##Save back only those commodities that
###         imputedSecondLoop=imputedSecondLoop[measuredItemCPC %in% secondLoop]
###         
###         ##The first save back should exclude those commodities imputed in the second round
###         
###         
###         ##SaveData(domain = sessionKey@domain,
###         ##         dataset = sessionKey@dataset,
###         ##         data =  imputedSecondLoop)
###         
###         
###         
###         outputToMerge=rbind(outputToMerge, outputToMergeSecondLoop)

message("Imputation Completed Successfully")