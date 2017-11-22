##' Function to compute shares (to express a derived commodities in its primary (or at least a higher) level.
##' This function has been written to be used in the DerivedCommodies_submodule which works level by level.
##' That's why this kind of shares are used to transform the derived into its direct parent.
##' that it is allocated in different productive processes.
##'
##' @param data input data.table: SUA table
##' @param tree Commodity tree, data.table
##' @param printNegativeAvailability It is TRUE if you want to produce csv file containing nagative availabilities
##' @param params computation parameters
##'
##' @export
##'



calculateShareDownUp=function(data,tree, printNegativeAvailability=FALSE, params)

{
##Checks
stopifnot(c(params$parentVar,params$geoVar,params$yearVar,params$elementVarSUA,params$value)  %in% colnames(data) )    
stopifnot(c(params$parentVar,params$geoVar,params$yearVar,params$childVar,params$extractVar,  params$level) %in% colnames(tree))
    
dataMergeTree=merge(data,tree, by=c(params$parentVar, params$geoVar,params$yearVar))
##allow.cartesian = TRUE)



##Simple availability that we interpret as FOOD PROCESSING: here I am using all the components
##dataMergeTree[, params$availVar := sum(ifelse(is.na(Value), 0, Value) *
##                                    ifelse(measuredElementSuaFbs == params$productionCode, 1,
##                                    ifelse(measuredElementSuaFbs == params$importCode, 1,
##                                    ifelse(measuredElementSuaFbs == params$exportCode , -1, 
##                                    ifelse(measuredElementSuaFbs == params$stockCode, -1,
##                                    ifelse(measuredElementSuaFbs == params$foodCode, -1,
##                                    ifelse(measuredElementSuaFbs == params$feedCode , -1,
##                                    ifelse(measuredElementSuaFbs == params$wasteCode, -1,
##                                    ifelse(measuredElementSuaFbs == params$seedCode, -1,
##                                    ifelse(measuredElementSuaFbs == params$industrialCode, -1,
##                                    ifelse(measuredElementSuaFbs == params$touristCode, -1, 0))))))))))),
##              by = c(params$geoVar,params$yearVar,params$parentVar,params$childVar)]




##Simple availability that we interpret as FOOD PROCESSING: this availability is based only on PRODUCTION, IMPORT and EXPORT
dataMergeTree[, params$availVar := sum(ifelse(is.na(Value), 0, Value) *
                                       ifelse(measuredElementSuaFbs == params$productionCode, 1,
                                       ifelse(measuredElementSuaFbs == params$importCode, 1,
                                       ifelse(measuredElementSuaFbs == params$exportCode , -1,0)))), 
                                  ##     ifelse(measuredElementSuaFbs == params$stockCode, -1,
                                  ##     ifelse(measuredElementSuaFbs == params$foodCode, -1,
                                  ##     ifelse(measuredElementSuaFbs == params$feedCode , -1,
                                  ##     ifelse(measuredElementSuaFbs == params$wasteCode, -1,
                                  ##     ifelse(measuredElementSuaFbs == params$seedCode, -1,
                                  ##     ifelse(measuredElementSuaFbs == params$industrialCode, -1,
                                  ##     ifelse(measuredElementSuaFbs == params$touristCode, -1, 0))))))))))),
              by = c(params$geoVar,params$yearVar,params$parentVar,params$childVar)]


##-------------------------------------------------------------------------------------------------------
##Deviate the negative agailability to be manually checked

if(printNegativeAvailability){
nagativeAvailability=dataMergeTree[availability<1]
nagativeAvailability=nagativeAvailability[,.(measuredItemParentCPC,	geographicAreaM49, availability,timePointYears)]
nagativeAvailability=unique(nagativeAvailability)
directory= "C:/Users/Rosa/Desktop/ProcessedCommodities/BatchExpandedItems/nagativeAvailability/"
dir.create(paste0(directory, lev), recursive=TRUE)
write.csv(nagativeAvailability, paste0(directory,lev, "/",currentGeo, "nagativeAvailability",".csv"), sep=";",row.names = F)
}
####------------------------------------------------------------------------------------------------------  

## I keep just the columns I need, note I am excluding the Element column because I am interested into te AVAILABITLITY
## I do not need all the components (prod, import, export, tourism, industial, stock... ) anymore. 
dataMergeTree=dataMergeTree[,c(params$parentVar,  params$geoVar, params$yearVar, params$childVar, params$extractVar,   
                              params$level, params$availVar, params$shareOldSystem), with=FALSE] 
## At this point we still have the availabilities repeated for each element of the original SUA table.
## For example if an Item X entered in the SUA table through 4 elements (prod, export, feed and stock), 
## we still have 4 rows with same availability
dataMergeTree = dataMergeTree[, list(availability = mean(get(params$availVar), na.rm = TRUE)),
                              by = c(params$parentVar,params$geoVar,params$yearVar,params$childVar,params$extractVar,  params$level, params$shareOldSystem)]



## In order to continue runnung the module even if some availability are lower than 0
## we make the assumption that: if the availability is equal to ZERO (or negative), it means that
## the productive process of its derived products cannot be activated (in any case it is the case
## to check the intial SUA table because there might be somethig wrong) 
dataMergeTree[get(params$availVar)<1,params$availVar:=0]				

## Express the whole availabilty of each parent in terms of child equivalent:
dataMergeTree[,availabilitieChildEquivalent:=get(params$availVar)* get(params$extractVar)]
## Sum of the availabilities express in terms of child eq. by child
dataMergeTree[, sumAvail:=sum(availabilitieChildEquivalent), by=c(params$childVar,params$yearVar,params$geoVar, params$shareOldSystem)]

## I create the column that has to be populated
dataMergeTree[,params$shareDownUp:=NA_real_]
dataMergeTree[,params$shareDownUp:=availabilitieChildEquivalent/sumAvail]

## We are currently using a version of the commodity-tree containing the shares coming from the old system,
## if the share exists I am keeping the one stored in the old system table instead of the one endogenously computed.
dataMergeTree[!is.na(get(params$shareOldSystem)),params$shareDownUp:=get(params$shareOldSystem)]

## Delete the columns I do not need anymore
dataMergeTree[,availabilitieChildEquivalent:=NULL]
dataMergeTree[,sumAvail:=NULL]

return(dataMergeTree)


}

