library(data.table)
files = dir("~/Desktop/FAOSTAT_download/", full.names = TRUE)
faostat = lapply(files, fread)
faostat = do.call("rbind", faostat)
allItems = faostat[, unique(get("Item Code"))]
allCPC = faoswsUtil::fcl2cpc(formatC(as.numeric(allItems), width = 4,
                                     flag = "0", format = "g"))
unique(allCPC[!is.na(allCPC)])
missingCodes = allItems[is.na(allCPC)]
missingCodes # These are all aggregates or groups
allPrimary = unique(allCPC[!is.na(allCPC)])
allPrimary = data.frame(measuredItemCPC = allPrimary)
write.csv(file = "~/Documents/Github/faoswsProduction/sandbox/allPrimaryCodes.csv",
          allPrimary, row.names = FALSE)
