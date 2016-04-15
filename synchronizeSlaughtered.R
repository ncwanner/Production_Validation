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
##
## NOTE (Michael): Sync from parents to children.
###############################################################################

cat("Beginning slaughtered synchronization script...\n")

library(data.table)
library(faosws)
library(faoswsUtil)
library(magrittr)

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
    } else if(Sys.info()[7] == "mk"){ # Josh work
        files = dir("R/", full.names = TRUE)
        SetClientFiles("~/.R/qa/")
        ## NOTE (Michael): We will use the Production shared drive as
        ##                 the files are not synced.
        ## R_SWS_SHARE_PATH = "/media/sws_qa_shared_drive/"
        R_SWS_SHARE_PATH = "/media/sws_prod_shared_drive/"
    } else {
        stop("Add your github directory here!")
    }
        
    ## Get SWS Parameters
    GetTestEnvironment(
        # baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
        # token = "916b73ad-2ef5-4141-b1c4-769c73247edd"
        baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
        token = "63479732-0657-4b50-8c8d-c41bef92841b"
    )
    sapply(files, source)
} else {
    cat("Working on SWS...\n")
}

startTime = Sys.time()

cat("Loading preliminary data...\n")

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
groupIncluded = meatGroupDT[measuredItemCPC %in% key@dimensions$measuredItemCPC@keys,
                            unique(meatGroup)]
requiredMeats = meatGroupDT[meatGroup %in% groupIncluded,
                            unique(measuredItemCPC)]
key@dimensions[[itemVar]]@keys = requiredMeats

## Update the measuredElements
key@dimensions[[elementVar]]@keys = unique(c(toSynch$measuredElementParent,
                                             toSynch$measuredElementChild))

# Execute the get data call.
cat("Pulling the data\n")
data = GetData(key = key)
# Remove missing values, as we don't want to copy those.
data = data[!flagObservationStatus == "M", ]
# Some figures shouldn't be overwritten, namely anything which is currently 
# offical.  Actually, we decided to overwrite these figures as well, as they are
# likely older official data.
# dontWrite = data[flagObservationStatus == "", ]
# Filter to only the parents, as we will only create children and do this
# creation using the parents.
allowedParents = unique(toSynch[, c("measuredItemParentCPC",
                                    "measuredElementParent"), with = FALSE])
setnames(allowedParents, c(itemVar, elementVar))
data = merge(data, allowedParents, by = colnames(allowedParents))

cat("Pulling the commodity trees...\n")

# Get the commodity tree (using faoswsUtil):
tree = getCommodityTree(geographicAreaM49 = as.character(unique(data$geographicAreaM49)),
                        timePointYears = as.character(unique(data$timePointYears)))
tree = tree[share > 0, ]

# Use the shares to convert the parent into the child:
setnames(tree, c("measuredItemParentCPC", "timePointYearsSP"),
         c("measuredItemCPC", "timePointYears"))
toWrite = merge(data, tree, by = c("geographicAreaM49", "timePointYears",
                                      "measuredItemCPC"), all.x = TRUE)
# Multiply value by share to get final value
toWrite[, c("Value", "share") := list(Value * share, NULL)]
# Assign to the children, not the parent
toWrite[, c("measuredItemCPC", "measuredItemChildCPC") :=
            list(measuredItemChildCPC, NULL)]
toWrite[, extractionRate := NULL]
# The measuredElement corresponds to the parent.  Correct this using the table
# from Tomasz, now on the share drive
toMerge = toSynch[, c("measuredItemChildCPC", "measuredElementChild"), with = FALSE]
setnames(toMerge, "measuredItemChildCPC", "measuredItemCPC")
toWrite = merge(toWrite, toMerge, by = "measuredItemCPC")
toWrite[, c("measuredElement", "measuredElementChild") :=
            list(measuredElementChild, NULL)]
# Don't write the data if there's currently an official figure in that spot
# toWrite = merge(toWrite, dontWrite, all.x = TRUE,
#                 by = c("measuredItemCPC", "geographicAreaM49",
#                        "timePointYears", "measuredElement"),
#                 suffixes = c("", ".skip"))
# toWrite = toWrite[is.na(Value.skip), ]
# toWrite[, c("Value.skip", "flagObservationStatus.skip",
#             "flagMethod.skip") := NULL]

# Convert factors to character
for(cname in c(areaVar, itemVar, elementVar, yearVar,
               "flagObservationStatus", "flagMethod")){
    toWrite[, c(cname) := as.character(get(cname))]
}



cat("Testing module ...\n")

sessionAnimalMeatMapping =
    getAnimalMeatMapping() %>%
    subsetAnimalMeatMapping(animalMeatMapping = .,
                            context = swsContext.datasets[[1]])


animalNumberDB =
    getAllAnimalNumber(sessionAnimalMeatMapping)

saveResult =
    toWrite %>%
    checkSlaughteredSynced(animalMeatMapping = sessionAnimalMeatMapping,
                           animalNumbers = animalNumberDB,
                           slaughteredNumbers = .) %>%
    .$slaughteredNumbers %>%
    checkProtectedData(dataToBeSaved = .) %>%
    SaveData(domain = swsContext.datasets[[1]]@domain,
             dataset = swsContext.datasets[[1]]@dataset,
             data = .)


## if(!inherits(moduleTest, "try-error")){
##     cat("Attempting to save to the database...\n")
##     saveResult = SaveData(domain = swsContext.datasets[[1]]@domain,
##                           dataset = swsContext.datasets[[1]]@dataset,
##                           data = toWrite)
## }

result = paste("Module completed with", saveResult$inserted + saveResult$appended,
      "observations updated and", saveResult$discarded, "problems.\n")
cat(result)

result
