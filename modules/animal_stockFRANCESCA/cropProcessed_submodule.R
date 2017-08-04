

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

primaryInvolved=c("01921.01" ,"01491.01" ,"01921.01" ,"0141"     ,"01445"    ,"01446"    ,"01443"    ,"01491.01" ,"01450"   ,"01444"    ,"01441"    ,"01802" ,  
"01801"    ,"01801"    ,"01802"    ,"01330"    ,"01921.01" ,"01491.01" ,"01460"    ,"0142"     ,"0141"    ,"01445"    ,"01443"    ,"01491.01",
"01449.01" ,"01449.90" ,"0112"     ,"0112"     ,"0111"     ,"0115"     ,"21511.01" ,"21512"    ,"21513"   ,"21514"    ,"21515"    ,"01921.01",
"01491.01" ,"01460"    ,"0142"     ,"21113.01" ,"21121"    ,"21111.01" ,"21511.01" ,"21511.01" ,"21512"   ,"21513"    ,"21514"    ,"21515" ,  
"21511.01" ,"21512"    ,"21514"    ,"0141"     ,"01445"    ,"01443"    ,"01491.01" ,"01444"    ,"01449.01","01449.90" ,"0111"     ,"0115" ,   
"01921.01" ,"01491.01" ,"01460"    ,"01460"    ,"0142"     ,"21113.01" ,"21113.01" ,"21113.01" ,"21111.01","21111.01" ,"21511.01" ,"21512" ,  
"21514"    ,"01450"    ,"0112"     ,"21113.01" ,"21111.01")

completeImputationKey=getCompleteImputationKey()
## FCL commodities taken from FAOSTAT (21)
cropProcessedFCL=c("0767","0329","0165","0331","0334","0261","0290","0268","0162","0051","1242",
                   "0244","0257","0237","0564","0271","0281","0252","0258","0256","0060")

## I need CPC codes
cropProcessedCPC=fcl2cpc(cropProcessedFCL)

##The three elements of the tripet are c("5510","5423","5327")
completeImputationKey@dimensions$measuredElement@keys=c("5510","5423","5327")
completeImputationKey@dimensions$measuredItemCPC@keys=cropProcessedCPC

dataProcessed=GetData(completeImputationKey)




##-------------------------------------------------------------------------------------------------------------------------------------



tree = getCommodityTree(timePointYears = completeImputationKey@dimensions$timePointYears@keys)

levels=getCommodityLevel(tree,"measuredItemParentCPC","measuredItemChildCPC")
setnames(levels, "node","measuredItemChildCPC")
tree=merge(tree, levels, by="measuredItemChildCPC")
tree = tree[!measuredItemParentCPC=="23670.01"] # All ER = NA (rows=3878)
tree = tree[!measuredItemParentCPC=="2351"] # All ER = NA 0.9200 0.9300 0.9650 0.9600 0.9350 0.9430 0.9346 (rows=3878)
tree = tree[!measuredItemParentCPC=="23511"] # All ER = NA (rows=3878)
tree = tree[!(measuredItemChildCPC=="2413"& measuredItemParentCPC %in% c("23520","23511.01","39160","24110"))] # NA 0.7 (rows=3878)
tree = tree[!(measuredItemChildCPC=="24110"& measuredItemParentCPC=="39160")] # NA 0.45 0.25 (rows=3878)
tree = tree[!(measuredItemChildCPC=="24490" & measuredItemParentCPC=="23511.01")] # NA 5 (rows=3878)
tree = tree[!(measuredItemChildCPC=="2351" & measuredItemParentCPC=="23512")] # All ER = NA (rows=3878)
tree = tree[!measuredItemChildCPC=="23511"] # NA 0.1 (rows=3878)
tree[measuredItemParentCPC=="01802" & measuredItemChildCPC=="23511.01", measuredItemChildCPC:="2351f"]
tree[measuredItemParentCPC=="01801" & measuredItemChildCPC=="23512", measuredItemChildCPC:="2351f"]
tree[measuredItemParentCPC=="23511.01", measuredItemParentCPC:="2351f"]
tree[measuredItemParentCPC=="23512", measuredItemParentCPC:="2351f"]
tree = tree[!(measuredItemParentCPC=="2351f" & measuredItemChildCPC == "2351f"),]

#### CRISTINA more corrections on sugar tree CRISTINA 7/7/2018
tree[measuredItemChildCPC=="2351f",extractionRate:=0.11]
tree[measuredItemParentCPC=="01343"& measuredItemChildCPC=="21419.01",extractionRate:=0.29]
tree[measuredItemParentCPC=="01491.01"& measuredItemChildCPC=="2165",extractionRate:=0.19]



tree[, extractionRate := ifelse(is.na(extractionRate),
                                mean(extractionRate, na.rm = TRUE),
                                extractionRate),
     by = c("measuredItemParentCPC", "measuredItemChildCPC")]
# If there's still no extraction rate, use an extraction rate of 1

# CRISTINA: I would chenge this to 0 because if no country report an extraction rate
# for a commodity, is probably because those commodities are not re;ated
# example: tree[geographicAreaM49=="276"&timePointYearsSP=="2012"&measuredItemParentCPC=="0111"]
# wheat germ shoul have ER max of 2% while here results in 100%

# tree[is.na(extractionRate), extractionRate := 1]
tree=tree[!is.na(extractionRate)]
## TO use collpseEdges we have to loop over country-year combinations
##tree=tree[geographicAreaM49=="380" & timePointYearsSP=="2010"]




##-------------------------------------------------------------------------------------------------------------------------------------
## I work on PARENT data 
load(file.path("C:/Users/Rosa/Favorites/Github/sws_project/StandardizationFiles/localFile", "260523_dataAllNew.RData"))


##When I subset the PARENT data, I need not only the primary, but all the intermediate derivative to calculate the availabilities
primaryInvolvedDescendents=list()

 for(i in seq_len(length(primaryInvolved)))
 { 
     primaryInvolvedDescendents[[i]]=data.table(getChildren( commodityTree = tree,
                                                   parentColname ="measuredItemParentCPC",
                                                   childColname = "measuredItemChildCPC",
                                                   topNodes =primaryInvolved[i] ))
  }

primaryInvolvedDescendents= rbindlist(primaryInvolvedDescendents)



dataSubset=data[measuredItemSuaFbs %in% primaryInvolvedDescendents$V1]
dataSubset=dataSubset[!is.na(measuredElementSuaFbs),]

dataSubset[, availability := sum(ifelse(is.na(Value), 0, Value) *
                               ifelse(measuredElementSuaFbs == "production", 1,
                               ifelse(measuredElementSuaFbs == "imports", 1,
                               ifelse(measuredElementSuaFbs == "exports" , -1, 0)))),
                               ##ifelse(measuredElementSuaFbs == "stockChange", 0,
                               ##ifelse(measuredElementSuaFbs == "food", 0,
                               ##ifelse(measuredElementSuaFbs == "feed" , 0,
                               ##ifelse(measuredElementSuaFbs == "loss", 0,
                               ##ifelse(measuredElementSuaFbs == "seed", 0,
                               ##ifelse(measuredElementSuaFbs == "industrial", 0,
                               ##ifelse(measuredElementSuaFbs == "tourist", 0, 0
                                
     by = c("geographicAreaM49","timePointYears","measuredItemSuaFbs")]

input=unique(dataSubset[,.(measuredItemSuaFbs,geographicAreaM49, timePointYears, availability)])


## choose in the tree only the 21 tree I am interested in:

tree=tree[measuredItemParentCPC %in%  primaryInvolvedDescendents$V1| measuredItemChildCPC %in% primaryInvolvedDescendents$V1]




##-------------------------------------------------------------------------------------------------------------------------------------

##I work on the tree
## I woud like to use the commodity tree



##unique(tree[measuredItemChildCPC %in% cropProcessedCPC, measuredItemParentCPC])


setnames(tree, "timePointYearsSP", "timePointYears")
setnames(input, "measuredItemSuaFbs","measuredItemParentCPC")

tree=merge(tree, input, by=c("measuredItemParentCPC","geographicAreaM49","timePointYears"))


uniqueLevels=unique(tree[,.(geographicAreaM49, timePointYears)])





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

calcAv=list()
for(i in seq_len(nrow(uniqueLevels))){
    filter = uniqueLevels[i, ]   
    
treeSubset=tree[filter,,on = c("geographicAreaM49", "timePointYears")]    
calcAvITER=calculateAvailability(treeSubset, params)
treeSubset[,availability:=NULL]
calcAv[[i]]=merge(treeSubset,calcAvITER,by=c("measuredItemParentCPC", "measuredItemChildCPC"))

}

calcAv=rbindlist(calcAv)
calcAv1 = calcAv[, list(availability = mean(availability)),
                   by = c("measuredItemChildCPC","measuredItemParentCPC", "measuredItemChildCPC", "geographicAreaM49", "timePointYears")]
##At the end of this process I do not have any duplcated rows
treeCollapsedOnlyCropPRocessed=list()

for(i in seq_len(nrow(uniqueLevels))){
filter = uniqueLevels[i, ]   
treeSubset=tree[filter,,on = c("geographicAreaM49", "timePointYears")]
treeCollapsed=collapseEdges(treeSubset,"measuredItemParentCPC", "measuredItemChildCPC", "extractionRate",keyCols=NULL )
##treeCollapsed=treeCollapsed[measuredItemChildCPC %in% cropProcessedCPC]

## I add a  column containing the numbers of times that a child appears (it means how many
##partents it has)

treeCollapsedOnlyCropPRocessed[[i]]=treeCollapsed[, n:= .N, by=measuredItemChildCPC]

## This was to find out the commodities 
##treeCropPrimary=treeCollapsed[measuredItemChildCPC %in% cropProcessedCPC]
##primaryInvolved=treeCropPrimary[,measuredItemParentCPC]
}

treeCollapsedOnlyCropPRocessed=rbindlist(treeCollapsedOnlyCropPRocessed)

treeCollapsedOnlyCropPRocessedTEST = treeCollapsedOnlyCropPRocessed[, list(extractionRate = mean(extractionRate)),
                   by = c("measuredItemParentCPC", "measuredItemChildCPC", "geographicAreaM49","timePointYears")]


## at this stage I have created an RData file instead of run it all the times

##OnlyOneParent=treeCollapsedOnlyCropPRocessed[n==1]
##parentOfOnlyOneParent=(OnlyOneParent[,measuredItemParentCPC])

calcAvResricted=calcAv[,.(measuredItemParentCPC ,measuredItemChildCPC, geographicAreaM49 ,timePointYears,availability)]
treeFinal=merge(calcAvResricted,treeCollapsedOnlyCropPRocessed, by=c("measuredItemParentCPC","measuredItemChildCPC","geographicAreaM49" ,"timePointYears"))


##here I can filter out derivatives commodities lower in the hierachy

##----------------------------------------------------------------------------------------------------------------------------------
##compute shareDownUp
treeFinal[,sumPositiveAvail:=sum(availability*ifelse(availability>0,1,0),na.rm=TRUE),by = c("measuredItemChildCPC","geographicAreaM49","timePointYears")]
treeFinal[, shareDownUp:=availability/ sumPositiveAvail]
##----------------------------------------------------------------------------------------------------------------------------------
## Put PARENT data Together with CHIL data and tree

setnames(dataProcessed, "measuredItemCPC", "measuredItemChildCPC")
dataProcessedPROD=dataProcessed[measuredElement=="5510"]

dataProcessedPROD=removeNonProtectedFlag(dataProcessedPROD, "Value", "flagObservationStatus", "flagMethod", normalised = TRUE)
dataProcessedPROD=merge(dataProcessedPROD, treeFinal,
                        by=c("measuredItemChildCPC","geographicAreaM49","timePointYears"))


dataProcessedPROD[,primaryEq:=(Value/extractionRate)*shareDownUp]


input=input[measuredItemParentCPC %in% parentOfOnlyOneParent,]

dataProcessedPROD=merge(dataProcessedPROD, input, by=c("measuredItemParentCPC","geographicAreaM49","timePointYears"))
dataProcessedPROD[,shareEndo:=NA_real_]
dataProcessedPROD[primaryEq!=0 & availability.x!=0,shareEndo:=primaryEq/availability.x]
##-------------------------------------------------------------------------------------------------------------------------------------
shareEndoImpute=dataProcessedPROD[,.(measuredItemParentCPC ,measuredItemChildCPC,geographicAreaM49 ,timePointYears,shareEndo)]


shareEndoParamenters=defaultImputationParameters()
shareEndoParamenters$imputationValueColumn="shareEndo"
shareEndoParamenters$imputationFlagColumn="shareEndoFlagObservationStatus"
shareEndoParamenters$imputationMethodColumn="shareEndoFlagMethod"
shareEndoParamenters$byKey=c("geographicAreaM49", "measuredItemChildCPC", "measuredItemParentCPC")
shareEndoParamenters$estimateNoData=FALSE


shareEndoParamenters$ensembleModels$defaultExp=NULL
##shareEndoParamenters$ensembleModels$defaultLogistic=NULL
##shareEndoParamenters$ensembleModels$defaultLoess=NULL
shareEndoParamenters$ensembleModels$defaultSpline=NULL
shareEndoParamenters$ensembleModels$defaultMars=NULL
shareEndoParamenters$ensembleModels$defaultMixedModel=NULL
##shareEndoParamenters$ensembleModels$defaultMovingAverage=NULL
##shareEndoParamenters$ensembleModels$defaultArima=NULL





shareEndoImpute[,shareEndoFlagObservationStatus:="T"]
shareEndoImpute[,shareEndoFlagMethod:="-"]


shareEndoImpute[is.na(shareEndo),shareEndoFlagObservationStatus:="M"]
shareEndoImpute[is.na(shareEndo),shareEndoFlagMethod:="u"]

shareEndoImpute[shareEndo<0, ":="(c("shareEndo","shareEndoFlagObservationStatus","shareEndoFlagMethod"), .(NA,"M","u"))]

##shareEndoImpute=shareEndoImpute[!is.na(measuredItemChildCPC)]
##shareEndoImputeTest=removeNoInfo(shareEndoImpute,"shareEndo","shareEndoFlagObservationStatus", byKey =  c("geographicAreaM49","measuredItemChildCPC"))

##shareEndoImputeTest=shareEndoImpute[, rep(containInfo(value = shareEndo,
##                                                      observationFlag = shareEndoFlagObservationStatus),
##                                          NROW(.SD)), by = c( "measuredItemParentCPC","measuredItemChildCPC", "geographicAreaM49")]

counts = shareEndoImpute[,
              sum(!is.na(shareEndo)),
              by = c(shareEndoParamenters$byKey)]
counts[V1!=0]
counts=counts[V1!=0]
counts=counts[,.(geographicAreaM49, measuredItemChildCPC ,measuredItemParentCPC)]

shareEndoImputeTestLAST=shareEndoImpute[counts, ,on=c("geographicAreaM49", "measuredItemChildCPC", "measuredItemParentCPC")]


##shareEndoImputeTestLAST=shareEndoImpute[shareEndoImputeTest[,V1],]

##shareEndoImputeTestLAST[,obsCount:=NULL]

shareEndoImputeTestAfterImputation=imputeVariable(shareEndoImputeTestLAST,shareEndoParamenters )




dataProcessedPROD[,timePointYears:=as.numeric(timePointYears)]
final=merge(dataProcessedPROD,shareEndoImputeTestAfterImputation, by=c("measuredItemParentCPC","measuredItemChildCPC","geographicAreaM49","timePointYears"))

##Understand why for some year It did not produce imputations:
##> final[measuredItemParentCPC=="0112" & measuredItemChildCPC=="21691.02" & geographicAreaM49=="170" ,]
##measuredItemParentCPC measuredItemChildCPC geographicAreaM49 timePointYears measuredElement Value flagObservationStatus flagMethod extractionRate share level n
##1:                  0112             21691.02               170           2000            5510    NA                     M          u        0.01875     1     2 1
##2:                  0112             21691.02               170           2001            5510    NA                     M          u        0.01875     1     2 1
##3:                  0112             21691.02               170           2002            5510    NA                     M          u        0.01875     1     2 1
##4:                  0112             21691.02               170           2003            5510    NA                     M          u        0.01875     1     2 1
##5:                  0112             21691.02               170           2004            5510    NA                     M          u        0.01875     1     2 1
##6:                  0112             21691.02               170           2005            5510    NA                     M          u        0.01875     1     2 1
##7:                  0112             21691.02               170           2006            5510    NA                     M          u        0.01875     1     2 1
##8:                  0112             21691.02               170           2007            5510    NA                     M          u        0.01875     1     2 1
##9:                  0112             21691.02               170           2008            5510    NA                     M          u        0.01875     1     2 1
##10:                  0112             21691.02               170           2009            5510    NA                     M          u        0.01875     1     2 1
##11:                  0112             21691.02               170           2010            5510    NA                     M          u        0.01875     1     2 1
##12:                  0112             21691.02               170           2011            5510    NA                     M          u        0.01875     1     2 1
##13:                  0112             21691.02               170           2012            5510  8930                                -        0.01875     1     2 1
##14:                  0112             21691.02               170           2013            5510  8901                                -        0.01875     1     2 1
##primaryEq availability shareEndo.x shareEndo.y shareEndoFlagMethod shareEndoFlagObservationStatus obsCount ValueImputed
##1:        NA      3132329          NA  0.08942148                   e                              I        2     5251.829
##2:        NA      2961476          NA  0.08726568                   e                              I        2     4845.661
##3:        NA      3360317          NA  0.08510988                   e                              I        2     5362.428
##4:        NA      3545119          NA  0.08295408                   e                              I        2     5514.039
##5:        NA      3894935          NA  0.08079828                   e                              I        2     5900.702
##6:        NA      4050944          NA  0.07864248                   e                              I        2     5973.306
##7:        NA      4772643          NA  0.07648669                   e                              I        2     6844.569
##8:        NA      5049668          NA  0.07433089                   e                              I        2     7037.744
##9:        NA      5049862          NA  0.07217509                   e                              I        2     6833.892
##10:        NA      4804062          NA          NA                   u                              M        2           NA
##11:        NA      5034608          NA          NA                   u                              M        2           NA
##12:        NA      4539328          NA          NA                   u                              M        2           NA
##13:  476266.7      5067853  0.09397800  0.09397800                   -                              T        2     8930.000
##14:  474720.0      5412473  0.08770852  0.08770852                   -                              T        2     8901.000
