library(faosws)
library(data.table)
library(ggplot2)

SetClientFiles(dir = "~/R certificate files/QA")
GetTestEnvironment(
    ## baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
    ## token = "7b588793-8c9a-4732-b967-b941b396ce4d"
    baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
    token = "44e5c005-dae3-4b4f-9654-0c29e0f39b73"
)

key = swsContext.datasets[[1]]
key@dimensions$timePointYears@keys = as.character(1991:2014)
d = GetData(key)
itemCodes = GetCodeList("agriculture", 'agriculture', "measuredItemCPC")
# dCrop = d[measuredItemCPC %in% itemCodes[type == "CRPR", code], ]
# write.csv(d, file = "~/Desktop/fullImputationSession.csv")
# write.csv(d, file = "~/Desktop/crprImputationSession.csv")

computeMean = function(x, obs, met, year){
    mean(x[!(obs == "I" & met == "e" & year >= 2010)])
}
computeSd = function(x, obs, met, year){
    sd(x[!(obs == "I" & met == "e" & year >= 2010)])
}

d[, mu := computeMean(Value, flagObservationStatus, flagMethod, timePointYears),
     by = c("geographicAreaM49", "measuredElement", "measuredItemCPC")]
d[, sd := computeSd(Value, flagObservationStatus, flagMethod, timePointYears),
     by = c("geographicAreaM49", "measuredElement", "measuredItemCPC")]
d[, nonImputedCount := sum(!flagObservationStatus %in% c("I", "M")),
  by = c("geographicAreaM49", "measuredElement", "measuredItemCPC")]
d[, sigmaCnt := abs(mu - Value)/sd]
qplot(d[flagObservationStatus == "I" & flagMethod == "e", sigmaCnt])
qplot(d[flagObservationStatus == "I" & flagMethod == "e", sigmaCnt]) + xlim(0, 10)
d[sigmaCnt == Inf, ]

qplot(d[flagObservationStatus == "I" & flagMethod == "e", sigmaCnt]) + xlim(0, 10) +
    labs(x = "# standard deviations from mean")

d[, coefficientOfVariation := round(sd / mu * 10)/10]
d[, coefficientOfVariation := ifelse(coefficientOfVariation > 1, ">1",
                                     coefficientOfVariation)]
ggplot(d[flagObservationStatus == "I" & flagMethod == "e", ]) + xlim(0, 10) +
    geom_bar(aes(x = sigmaCnt)) +
    labs(x = "# standard deviations from mean") +
    facet_wrap( ~ coefficientOfVariation, scale = "free_y")

d[sigmaCnt >= 100 & sigmaCnt < Inf, ] # Maybe logistic should have extrapolation range of 0?
d[sigmaCnt >= 40 & sigmaCnt < 100, ] # No problems
d[sigmaCnt >= 30 & sigmaCnt < 40 & nonImputedCount > 1, ] # No problems
d[sigmaCnt >= 20 & sigmaCnt < 30 & nonImputedCount > 1, ] # 

toSave = copy(d)
toSave[, c("mu", "sd", "sigmaCnt", "coefficientOfVariation",
           "officialObsCount") := NULL]
prodElements = GetCodeTree("agriculture", "agriculture", "measuredElement")
prodElements = prodElements[parent == "51", strsplit(children, split = ", ")[[1]]]
prodElements = prodElements[!prodElements %in% c("55100", "55101")] # Calculated differently in old methodology
toSave = toSave[measuredElement %in% prodElements, ]
## Datasets with low counts of observations
for(i in 1:5){
    write.csv(toSave[nonImputedCount == i & !flagObservationStatus %in% c("I", "M"), ],
              file = paste0("/media/T_drive/Team_working_folder/A/Production Imputation/data_",
                            i, "_prod_observations.csv"), row.names = FALSE)
}
