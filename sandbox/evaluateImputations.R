library(faosws)
library(data.table)
library(ggplot2)

SetClientFiles(dir = "~/R certificate files/QA")
GetTestEnvironment(
    ## baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
    ## token = "7b588793-8c9a-4732-b967-b941b396ce4d"
    baseUrl = "https://hqlqasws1.hq.un.fao.org:8181/sws",
    token = "0ed6583e-17b4-4334-8336-894273e44470"
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
d[, sigmaCnt := abs(mu - Value)/sd]
qplot(d[flagObservationStatus == "I" & flagMethod == "e", sigmaCnt])
qplot(d[flagObservationStatus == "I" & flagMethod == "e", sigmaCnt]) + xlim(0, 10)
d[sigmaCnt == Inf, ]

qplot(d[flagObservationStatus == "I" & flagMethod == "e", sigmaCnt]) + xlim(0, 10) +
    labs(x = "# standard deviations from mean")
ggplot(d[flagObservationStatus == "I" & flagMethod == "e" & sigmaCnt < Inf, ], aes(x = sd, y = sigmaCnt)) +
    geom_point(alpha = .1) + scale_y_log10() + scale_x_log10()

d[, coefficientOfVariation := round(sd / mu * 10)/10]
d[, coefficientOfVariation := ifelse(coefficientOfVariation > 1, ">1",
                                     coefficientOfVariation)]
ggplot(d[flagObservationStatus == "I" & flagMethod == "e", ]) + xlim(0, 10) +
    geom_bar(aes(x = sigmaCnt)) +
    labs(x = "# standard deviations from mean") +
    facet_wrap( ~ coefficientOfVariation)
