


getTree=function(){
    ##Commodity Tree
    
    
    
    tree = getCommodityTree(timePointYears = completeImputationKey@dimensions$timePointYears@keys)
    
    
    ### Francesca meat correction
    meat=c("21118.01","21139","21111.01" ,"21112","21113.01","21114","21115","21116","21117.01","21117.02","21118.02","21118.03","21119.01","21121","21122",
           "21123","21124","21125","21131","21132","21133","21134","21135","21136","21137","21137","21138","21138","21138","21141",
           "21142","21143","21144","21145","21170.01")
    animals= unique(tree[measuredItemChildCPC %in% meat, measuredItemParentCPC])
    
    
    tree=tree[!measuredItemParentCPC %in% animals,]
    
    ### Francesca cassava correction
    tree=tree[measuredItemParentCPC!="01520",]
    
    ############################################################
    ##                                                        ##
    ##                       Meat                             ##
    ##                                                        ##
    ############################################################
    
    
    ##This connections are not meaningful
    
    tree= tree[!(measuredItemParentCPC=="21511.02" & measuredItemChildCPC %in% c("21184.02","21113.02"))]
    
    tree= tree[!(measuredItemParentCPC=="21113.02" & measuredItemChildCPC %in% c("21181"))]
    tree[measuredItemParentCPC=="21121" & measuredItemChildCPC %in% c("23991.04"), extractionRate:=1]
    
    
    
    
    
    
    
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
    
    ## TO use collpseEdges we have to loop over country-year combinations
    ##tree=tree[geographicAreaM49=="380" & timePointYearsSP=="2010"]
    
    ## Francesca add some Extraction rates always equal to zero
    
    
    tree[measuredItemChildCPC=="22221.02", extractionRate:=0.4]
    tree[measuredItemChildCPC=="22230.04", extractionRate:=0.1]
    tree[measuredItemChildCPC=="22249.02", extractionRate:=0.1]
    
    tree=tree[!is.na(extractionRate)]
    
    tree = tree[!(measuredItemChildCPC=="22230.04" & measuredItemParentCPC=="22230.03")]
    
    
    
    
    ##Francesca: avoid that the same item appears in more that one single level
    
    tree = tree[!(measuredItemParentCPC=="02211" & measuredItemChildCPC == "22212"),]
    tree = tree[!(measuredItemParentCPC=="22211" & measuredItemChildCPC == "22222.01"),]
    
    
    tree = tree[!(measuredItemParentCPC=="22211" & measuredItemChildCPC == "22221.01"),]
    tree = tree[!(measuredItemParentCPC=="22212" & measuredItemChildCPC == "22221.01"),]
    
    tree = tree[!(measuredItemParentCPC=="22110.02" & measuredItemChildCPC == "22230.01"),]
    
    
    
    tree = tree[!(measuredItemParentCPC=="22110.02" & measuredItemChildCPC == "22130.01"),]
    tree = tree[!(measuredItemParentCPC=="22110.05" & measuredItemChildCPC == "22130.01"),]
    
    
    tree = tree[!(measuredItemParentCPC=="22110.05" & measuredItemChildCPC == "22254"),]
    
    
    
    tree = tree[!(measuredItemParentCPC=="21529.03" & measuredItemChildCPC == "21523"),]
    
    
    
    levels=findProcessingLevel(tree,"measuredItemParentCPC","measuredItemChildCPC")
    setnames(levels, "temp","measuredItemParentCPC")
    tree=merge(tree, levels, by="measuredItemParentCPC", all.x=TRUE)
    
    
    
}