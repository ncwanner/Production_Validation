############################################################################### 
##Title: Impute Slaughtered Module for SWS
##
##Author: Josh Browning
##
##The animals slaughtered for production of meat, offals, fats and hides must be
##available before running the production imputation code.  These numbers, 
##however, are not guaranteed to be available, and in the case of missing data, 
##an imputation method must be applied.  The decision was to use the production 
##figures of meat, if available, to compute the missing animals slaughtered.  If
##these figures are also missing, they should be imputed using the production 
##imputation methodology.  Of course, in the case of currently available data in
##the animal element, that data should be transferred to the quantity of animals
##slaughtered for meat and then the imputation ran.  We also decided to save the
##imputations for meat so as to retain consistency with the animal figures. 
###############################################################################

cat("Beginning impute slaughtered script...")

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
    } else {
        stop("Add your github directory here!")
    }

    ## Get SWS Parameters
    GetTestEnvironment(
        # baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
        # token = "916b73ad-2ef5-4141-b1c4-769c73247edd"
        baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
        token = "aeaaa18a-1ec3-4b9e-96ee-cc3a86f8ff98"
    )
    sapply(files, source)
} else {
    cat("Working on SWS...\n")
}

startTime = Sys.time()

cat("Loading preliminary data...")

toProcess = fread(paste0(R_SWS_SHARE_PATH,
                         "/browningj/production/slaughtered_synchronized.csv"),
                  colClasses = "character")
# Filter to just meats => CPC code like 2111* or 2112* (21111.01, 21112, ...)
toProcess = toProcess[grepl("^211(1|2).*", measuredItemChildCPC), ]

## Read the data.  The years and countries provided in the session are used, and
## the commodities in the session are somewhat considered. For example, if 02111
## (Cattle) is in the session, then the session will be expanded to also include
## 21111.01 (meat of cattle, fresh or chilled), 21151 (edible offal of cattle,
## fresh, chilled or frozen), 21512 (cattle fat, unrendered), and 02951.01 (raw
## hides and skins of cattle).  The measured element dimension of the session is
## simply ignored.

## Expand the session to include missing meats
key = swsContext.datasets[[1]]
rowsIncluded = toProcess[, measuredItemParentCPC %in% key@dimensions$measuredItemCPC@keys |
                           measuredItemChildCPC %in% key@dimensions$measuredItemCPC@keys]
requiredMeats = toProcess[rowsIncluded, c(measuredItemParentCPC, measuredItemChildCPC)]
key@dimensions[[itemVar]]@keys = requiredMeats

## Update the measuredElements
key@dimensions[[elementVar]]@keys =
    unique(toProcess[rowsIncluded, c(measuredElementParent, measuredElementChild)])

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

message = paste("Module completed with", saveResult$inserted + saveResult$appended,
      "observations updated and", saveResult$discarded, "problems.")
cat(message)

message