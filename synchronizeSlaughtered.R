###############################################################################
## Title: Stock Synchronization Module for SWS
## 
## Author: Josh Browning
## 
## This module is used to synchronize various cells in the production database. 
## For example, Cattle (02111) has element 5315 (animals slaughtered) and this
## should be the same as 5320 (animals slaughtered) for commodities 21111.01
## (meat of cattle, fresh or chilled), 21151 (edible offal of cattle, fresh,
## chilled or frozen), 21512 (cattle fat, unrendered), and 02951.01 (raw hides
## and skins of cattle).
###############################################################################

library(data.table)
library(faosws)

areaVar = "geographicAreaM49"
yearVar = "timePointYears"
itemVar = "measuredItemCPC"
elementVar = "measuredElement"

## set up for the test environment and parameters
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")
DEBUG_MODE = Sys.getenv("R_DEBUG_MODE")

if(!exists("DEBUG_MODE") || DEBUG_MODE == ""){
    cat("Not on server, so setting up environment...\n")

    if(Sys.info()[7] == "josh"){ # Josh work
        files = dir("~/Documents/Github/faoswsProduction/R/",
                    full.names = TRUE)
        SetClientFiles("~/R certificate files/QA/")
        R_SWS_SHARE_PATH = "/media/hqlprsws1_qa/"
    } else {
        stop("Add your github directory here!")
    }
        
    ## Get SWS Parameters
    GetTestEnvironment(
        # baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
        # token = "916b73ad-2ef5-4141-b1c4-769c73247edd"
        baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
        token = "f10e9805-1bbe-4865-84c4-aa3f8e1a66ea"
    )
    sapply(files, source)
} else {
    cat("Working on SWS...\n")
}

startTime = Sys.time()

toSynch = fread(paste0(R_SWS_SHARE_PATH,
        "/browningj/production/slaughtered_synchronized.csv"),
        colClasses = "character")
meatGroups = split(x = toSynch[, measuredItemChildCPC],
                   f = toSynch[, measuredItemParentCPC])
meatGroups = lapply(1:length(meatGroups), function(i){
    c(meatGroups[[i]], names(meatGroups)[i])
})

meatGroupDT = lapply(1:length(meatGroups), function(i){
    data.table(meatGroup = i, measuredItemCPC = meatGroups[[i]])
})
meatGroupDT = do.call("rbind", meatGroupDT)

## Read the data.  The years and countries provided in the session are used, and
## the commodities in the session are somewhat considered. For example, if 02111
## (Cattle) is in the session, then the session will be expanded to also include
## 21111.01 (meat of cattle, fresh or chilled), 21151 (edible offal of cattle,
## fresh, chilled or frozen), 21512 (cattle fat, unrendered), and 02951.01 (raw
## hides and skins of cattle).  The measured element dimension of the session is
## simply ignored.

## Expand the session to include missing meats
key = swsContext.datasets[[1]]
groupIncluded = sapply(meatGroups, function(x){
    any(x %in% key@dimensions[[itemVar]]@keys)
})
requiredMeats = meatGroups[groupIncluded]
requiredMeats = do.call("c", requiredMeats)
key@dimensions[[itemVar]]@keys = requiredMeats

## Update the measuredElements
key@dimensions[[elementVar]]@keys = unique(c(toSynch$measuredElementParent,
                                             toSynch$measuredElementChild))

# Execute the get data call. 
history = GetHistory(key = key)
fullHistory = copy(history)
history = history[, c(areaVar, elementVar, itemVar, yearVar, "StartDate",
                      "Value", "flagObservationStatus", "flagMethod"),
                  with = FALSE]
# Remove missing values, as we don't want to copy those.
history = history[!flagObservationStatus == "M", ]
# Filter to only the appropriate element codes for each item code
allowedParents = toSynch[, c("measuredItemParentCPC", "measuredElementParent"),
                         with = FALSE]
setnames(allowedParents, c(itemVar, elementVar))
allowedChildren = toSynch[, c("measuredItemChildCPC", "measuredElementChild"),
                          with = FALSE]
setnames(allowedChildren, c(itemVar, elementVar))
allowedPairs = unique(rbind(allowedParents, allowedChildren))
history = merge(history, allowedPairs, by = colnames(allowedPairs))

##' Select Data
##' 
##' This function selects one observation out of a passed data.table to use as 
##' the 'best' observation.  We currently keep the most recent official figure, 
##' and if there is no official figure, then we keep the most recent figure.
##' 
##' @param historyData The data.table containing historical observations, pulled
##'   using faosws::GetHistory
##' 
##' @return A data.table with one row containing the most recent observation.
##' 
selectData = function(historyData){
    # Official first.  If there is official data, we can drop all other
    # observations.
    if(any(historyData$flagObservationStatus == "")){
        historyData = historyData[flagObservationStatus == "", ]
    }
    
    # Select the most recent observation
    out = historyData[StartDate == max(StartDate), ]
    if(nrow(out) > 1){
        # Strip keys as they cause unwanted behaviour with unique
        setkeyv(out, NULL)
        out = unique(out)
        if(nrow(out) > 1){
            if(!all(out$Value[1] == out$Value)){
                stop("Multiple unique observations selected with different ",
                     "values!  Are you using the appropriate byKey?  If not, ",
                     "you may need an additional tie-breaker.")
            } else {
                out = out[1, ]
#                 warning("Multiple unique observations selected with the same ",
#                         "value, simply selecting the first.")
            }
        }
    }
    return(out)
}

##' Expand Selected Data
##' 
##' This function takes the data that was selected by selectData and expands it 
##' to all relevant commodity/element codes.
##' 
##' @param selectedData The data.table containing historical observations after
##'   being processed by selectData.
##'   
##' @return A data.table with the appropriate value/flags copied to all paired elements.
##'   
expandSelected = function(selectedHistory){
    selectedHistory = merge(selectedHistory, meatGroupDT, by = "meatGroup",
                            suffixes = c(".old", ""), allow.cartesian = TRUE)
    # Delete the old CPC code (as the new code has all the codes instead of just
    # the one) and get rid of elementVar so we can update it.
    selectedHistory[, c(paste0(itemVar, ".old"), elementVar) := NULL]
    merge(selectedHistory, allowedPairs, by = itemVar)
}

# Define a column to identify which group a particular meat commodity belongs 
# in.  This will be used for later grouping.  The below sapply checks to see 
# which meatGroup a particular CPC is in.  This creates a logical vector of 
# length 16, and multiplying this vector by 1:length(meatGroup) produces the index of interest.  This can then be used for grouping.
history[, meatGroup := sapply(meatGroups, function(group){
    get(itemVar) %in% group}) %*% 1:length(meatGroups)]

# selectedHistory = history[1:1000, selectData(.SD), by = c(areaVar, yearVar, "meatGroup")]
selectedHistory = history[, selectData(.SD), by = c(areaVar, yearVar, "meatGroup")]
out = expandSelected(selectedHistory)
out = out[, c(areaVar, itemVar, elementVar, yearVar,
              "Value", "flagObservationStatus", "flagMethod"), with = FALSE]
# Convert factors to character
for(cname in c(areaVar, itemVar, elementVar, yearVar,
               "flagObservationStatus", "flagMethod")){
    out[, c(cname) := as.character(get(cname))]
}

saveResult = SaveData(domain = swsContext.datasets[[1]]@domain,
                      dataset = swsContext.datasets[[1]]@dataset,
                      data = out)

paste("Module completed with", saveResult$inserted + saveResult$appended,
      "observations updated and", saveResult$discarded, "problems.")
