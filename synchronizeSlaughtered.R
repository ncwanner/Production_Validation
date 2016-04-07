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

cat("Beginning slaughtered synchronization script...")

library(data.table)
library(faosws)
library(faoswsUtil)

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

cat("Loading preliminary data...")

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
cat("Pulling the data")
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

cat("Pulling the commodity trees...")

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

cat("Attempting to save to the database...")

saveResult = SaveData(domain = swsContext.datasets[[1]]@domain,
                      dataset = swsContext.datasets[[1]]@dataset,
                      data = toWrite)

result = paste("Module completed with", saveResult$inserted + saveResult$appended,
      "observations updated and", saveResult$discarded, "problems.")
cat(result)

result




getMeatReferenceFile = function(){
    fread(paste0(R_SWS_SHARE_PATH,
                 "/browningj/production/slaughtered_synchronized.csv"),
          colClasses = "character")
}

meatReferenceData = getMeatReferenceFile()

subsetMeatReferenceData = function(meatReferenceData, context){
    selectedItems = context@dimensions$measuredItemCPC@keys
    meatReferenceData[measuredItemParentCPC %in% selectedItems |
                      measuredItemChildCPC %in% selectedItems, ]
}
sessionMeatReferenceData =
    subsetMeatReferenceData(meatReferenceData, swsContext.datasets[[1]])


getAllAnimalNumber = function(referenceData){
    mainKey = getMainKey(swsContext.datasets[[1]]@dimensions$timePointYears@keys)
    mainKey@dimensions$measuredItemCPC@keys =
        referenceData$measuredItemParentCPC
    mainKey@dimensions$measuredElement@keys =
        referenceData$measuredElementParent
    animalNumbers = GetData(mainKey)
    setnames(animalNumbers,
             old = c("measuredItemCPC", "measuredElement"),
             new = c("measuredItemParentCPC", "measuredElementParent"))
    animalNumbers
}
allAnimalNumbers = getAllAnimalNumber(sessionMeatReferenceData)

getAllSlaughteredNumber = function(referenceData){
    mainKey = getMainKey(swsContext.datasets[[1]]@dimensions$timePointYears@keys)
    mainKey@dimensions$measuredItemCPC@keys =
        referenceData$measuredItemChildCPC
    mainKey@dimensions$measuredElement@keys =
        referenceData$measuredElementChild
    slaughteredNumbers = GetData(mainKey)
    setnames(slaughteredNumbers,
             old = c("measuredItemCPC", "measuredElement"),
             new = c("measuredItemChildCPC", "measuredElementChild"))
    slaughteredNumbers
}
allSlaughteredNumbers = getAllSlaughteredNumber(sessionMeatReferenceData)

checkSlaughteredSynced = function(referenceData, animalNumbers,
                                slaughteredNumbers){
    ## Function to check whether the slaughtered animal and animal
    ## number are in sync.
    ##
    ## NOTE (Michael): Need to subset for session, this function
    ##                 should be the data requirement of the module.
    ##
    ## NOTE (Michael): Should we check for missing values? I think
    ##                 missing values should be synced, unless both
    ##                 are missing.

    referenceWithAnimalData =
        merge(animalNumbers, referenceData,
              by = intersect(colnames(animalNumbers),
                             colnames(referenceData)),
              allow.cartesian = TRUE, all = TRUE)

    setnames(referenceWithAnimalData,
             old = c("Value", "flagObservationStatus", "flagMethod"),
             new = paste0("animal_",
                          c("Value", "flagObservationStatus", "flagMethod")))

    referenceWithSlaughteredData =
        merge(slaughteredNumbers, referenceData,
              by = intersect(colnames(slaughteredNumbers),
                             colnames(referenceData)),
              allow.cartesian = TRUE, all = TRUE)

    setnames(referenceWithSlaughteredData,
             old = c("Value", "flagObservationStatus", "flagMethod"),
             new = paste0("slaughtered_",
                          c("Value", "flagObservationStatus", "flagMethod")))

    referenceWithAllData <<-
        merge(referenceWithAnimalData, referenceWithSlaughteredData,
              intersect(colnames(referenceWithAnimalData),
                        colnames(referenceWithSlaughteredData)))


    ## referenceWithAllData =
    ##     merge(referenceWithParentData, slaughteredNumbers,
    ##           by = intersect(colnames(slaughteredNumbers),
    ##                          colnames(referenceWithParentData)),
    ##           all = TRUE)
    ## setnames(referenceWithAllData,
    ##          old = c("Value", "flagObservationStatus", "flagMethod"),
    ##          new = paste0("slaughtered_",
    ##                       c("Value", "flagObservationStatus", "flagMethod")))
    ## referenceWithAllData <<- referenceWithAllData
    with(referenceWithAllData,
         if(!all(animal_Value == slaughtered_Value))
             stop("Not all values were synced")
         )
}

checkSlaughteredSynced(sessionMeatReferenceData, allAnimalNumbers,
                       allSlaughteredNumbers)




getMeatReferenceFile() %>%
    subsetMeatReferenceData(context = swsContext.datasets[[1]]) %>%
    {
        ## Get the data based on the referenced file
        list(referenceData = .,
             animalNumbers = getAllAnimalNumber(referenceData = .),
             slaughteredNumbers = getAllSlaughteredNumber(referenceData = .))
    } %$%
    checkSlaughteredSynced(referenceData, animalNumbers, slaughteredNumbers)
