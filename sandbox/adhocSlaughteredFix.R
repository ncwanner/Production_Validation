## In the first questionaire import into the new system, slaughtered animal 
## numbers were mistakenly placed under the meat CPC code (e.g. 21111.01 for 
## cattle) rather than the animal CPC (e.g. 02111 for cattle).  This script is 
## used to take an export of the system at that point and convert the meat
## values back into the animal values.

library(data.table)
library(ggplot2)

d = fread("~/Documents/Github/faoswsProduction/sandbox/slaughteredData.csv", colClasses = "char")
d[, Value := as.numeric(Value)]
badUpload = d[flagObservationStatus == "" & flagMethod == "q" &
                  measuredElement %in% c("5320", "5321"), ]
setnames(badUpload, c("Value", "flagObservationStatus", "flagMethod"),
         c("ValueMeat", "flagObservationStatusMeat", "flagMethodMeat"))
badUpload[, measuredElement := ifelse(measuredElement == "5320", "5315", "5316")]
map = fread("/media/hqlprsws1_qa/browningj/production/slaughtered_synchronized.csv",
            colClasses = "char")
setnames(map, "measuredItemChildCPC", "measuredItemCPC")
badUpload[, measuredItemCPC := as.character(measuredItemCPC)]
badUpload = merge(badUpload, map[, c("measuredItemCPC", "measuredItemParentCPC"), with = FALSE],
      by = "measuredItemCPC")
badUpload[, measuredItemCPC := measuredItemParentCPC]
badUpload[, c("Item", "Element", "measuredItemParentCPC") := NULL]
compare = merge(badUpload, d, by = intersect(colnames(d), colnames(badUpload)),
                all.x = TRUE)
setnames(compare, "Geographic Area", "Geographic.Area")

compare[is.na(Value) | ValueMeat != Value, .N, flagObservationStatus]
ggplot(compare[flagObservationStatus == "" & Value != ValueMeat, ], aes(x = ValueMeat, y = Value)) +
    geom_point() + scale_x_log10() + scale_y_log10()

setnames(compare, c("Value", "flagObservationStatus", "flagMethod"),
         c("Value_old", "flagObservationStatus_old", "flagMethod_old"))
setnames(compare, c("ValueMeat", "flagObservationStatusMeat", "flagMethodMeat"),
         c("Value", "flagObservationStatus", "flagMethod"))
compare[, pctError := abs(Value - Value_old) / Value_old]
compare = compare[order(-pctError), ]
compare = compare[, c("geographicAreaM49", "measuredItemCPC", "measuredElement",
                      "timePointYears", "Value", "flagObservationStatus",
                      "flagMethod"), with = FALSE]

write.csv(compare, file = "~/Documents/Github/faoswsProduction/sandbox/slaughtered_after_fix.csv", row.names = FALSE)
compareMeta = copy(compare)
setnames(compareMeta, c("Value", "flagObservationStatus", "flagMethod"),
         c("Data Value", "Status", "Method"))
compareMeta[, "Metadata Type" := "[GENERAL] General"]
compareMeta[, "Metadata Id" := 1]
compareMeta[, "Metadata Element Type" := "[COMMENT] Comment"]
compareMeta[, Language := "[en] English"]
compareMeta[, Value := "Copied from corresponding meat commodity as erroneously imported from questionaire"]
compareMeta = compareMeta[, c("geographicAreaM49", "measuredItemCPC", "measuredElement",
                      "timePointYears", "Metadata Type", "Metadata Id",
                      "Metadata Element Type", "Language", "Value", "Data Value",
                      "Status", "Method"), with = FALSE]
write.csv(compareMeta, file = "~/Documents/Github/faoswsProduction/sandbox/slaughteredMeta_after_fix.csv",
          row.names = FALSE)

deleteObs = d[flagObservationStatus == "" & flagMethod == "q" &
                  measuredElement %in% c("5320", "5321"), ]
deleteObs[, Value := 0]
deleteObs[, flagObservationStatus := "M"]
deleteObs[, flagMethod := "u"]
deleteObs[, c("Geographic Area", "Item", "Element", "Year") := NULL]
write.csv(deleteObs, file = "~/Documents/Github/faoswsProduction/sandbox/slaughteredMeat_after_fix.csv", row.names = FALSE)
deleteMeta = copy(deleteObs)
setnames(deleteMeta, c("Value", "flagObservationStatus", "flagMethod"),
         c("Data Value", "Status", "Method"))
deleteMeta[, "Metadata Type" := "[GENERAL] General"]
deleteMeta[, "Metadata Id" := 1]
deleteMeta[, "Metadata Element Type" := "[COMMENT] Comment"]
deleteMeta[, Language := "[en] English"]
deleteMeta[, Value := "Copied from corresponding meat commodity as erroneously imported from questionaire"]
deleteMeta = deleteMeta[, c("geographicAreaM49", "measuredItemCPC", "measuredElement",
                            "timePointYears", "Metadata Type", "Metadata Id",
                            "Metadata Element Type", "Language", "Value", "Data Value",
                            "Status", "Method"), with = FALSE]
write.csv(deleteMeta, file = "~/Documents/Github/faoswsProduction/sandbox/slaughteredMeatMeta_after_fix.csv",
          row.names = FALSE)
