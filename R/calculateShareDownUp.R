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



##Simple availability that we interpret as FOOD PROCESSING
dataMergeTree[, params$availVar := sum(ifelse(is.na(Value), 0, Value) *
                                    ifelse(measuredElementSuaFbs == params$productionCode, 1,
                                    ifelse(measuredElementSuaFbs == params$importCode, 1,
                                    ifelse(measuredElementSuaFbs == params$exportCode , -1, 
                                    ifelse(measuredElementSuaFbs == params$stockCode, -1,
                                    ifelse(measuredElementSuaFbs == params$foodCode, -1,
                                    ifelse(measuredElementSuaFbs == params$feedCode , -1,
                                    ifelse(measuredElementSuaFbs == params$wasteCode, -1,
                                    ifelse(measuredElementSuaFbs == params$seedCode, -1,
                                    ifelse(measuredElementSuaFbs == params$industrialCode, -1,
                                    ifelse(measuredElementSuaFbs == params$touristCode, -1, 0))))))))))),
              by = c(params$geoVar,params$yearVar,params$parentVar,params$childVar)]

##-------------------------------------------------------------------------------------------------------
##Deviate the negative agailability to be manually checked

if(printNegativeAvailability){
nagativeAvailability=dataMergeTree[availability<1]
nagativeAvailability=nagativeAvailability[,.(measuredItemParentCPC,	geographicAreaM49, availability,timePointYears)]
nagativeAvailability=unique(nagativeAvailability)
directory= "C:/Users/Rosa/Desktop/DERIVATIVES/negativeAvailability/"
dir.create(paste0(directory, lev), recursive=TRUE)
write.csv(nagativeAvailability, paste0(directory,lev, "/",currentGeo, "nagativeAvailability",".csv"), sep=";",row.names = F)
}
####------------------------------------------------------------------------------------------------------  

dataMergeTree=dataMergeTree[,c(params$parentVar,  params$geoVar, params$yearVar, params$childVar, params$extractVar,   
                              params$level, params$availVar), with=FALSE] 
dataMergeTree = dataMergeTree[, list(availability = mean(get(params$availVar), na.rm = TRUE)),
                              by = c(params$parentVar,params$geoVar,params$yearVar,params$childVar,params$extractVar,  params$level)]

#dataMergeTree[, params$availVar := mean(get(params$availVar), na.rm = TRUE),
#                              by = c(params$parentVar,params$geoVar,params$yearVar,params$childVar,params$extractVar,  params$level)]

## In order to continue runnung the module even if some availability are lower than 0
## we make the assumption that:

dataMergeTree[get(params$availVar)<1,params$availVar:=0]				

dataMergeTree[,availabilitieChildEquivalent:=get(params$availVar)* get(params$extractVar)]
dataMergeTree[, sumAvail:=sum(availabilitieChildEquivalent), by=c(params$childVar,params$yearVar,params$geoVar)]

dataMergeTree[,params$shareDownUp:=NA]

dataMergeTree[,params$shareDownUp:=availabilitieChildEquivalent/sumAvail]
dataMergeTree[,availabilitieChildEquivalent:=NULL]
dataMergeTree[,sumAvail:=NULL]

return(dataMergeTree)

return(params)
}

