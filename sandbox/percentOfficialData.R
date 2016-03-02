library(ggplot2)
library(gridExtra)
library(faosws)
library(faoswsUtil)
library(data.table)

R_SWS_SHARE_PATH = "/media/hqlprsws2_prod/"

SetClientFiles("~/R certificate files/Production/")
GetTestEnvironment(
    baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
    token = "ac1d3ae0-20f7-4632-b59b-d9b954e5a061"
)

files = dir("~/Documents/Github/faoswsProduction/R", full.names = TRUE)
sapply(files, source)

fullKey = getMainKey(years = 1991:2011)
d = GetData(fullKey)
yieldFormula = ReadDatatable(table = "item_type_yield_elements")
d[, itemType := ifelse(measuredElement %in% yieldFormula[, element_31], "input",
                ifelse(measuredElement %in% yieldFormula[, element_41], "productivity",
                ifelse(measuredElement %in% yieldFormula[, element_51], "production",
                       NA)))]
# Remove biological/indigenous
d = d[nchar(measuredElement) == 4, ]

aggFun = function(x){
    if(length(x) == 0)
        return(NA)
    if(length(x) > 1)
        return(sample(x, size = 1))
    else
        return(x)
}
dVal = dcast.data.table(d, geographicAreaM49 + measuredItemCPC + timePointYears ~ itemType,
                        value.var = "Value", fun.aggregate = aggFun)
dFlag = dcast.data.table(d, geographicAreaM49 + measuredItemCPC + timePointYears ~ itemType,
                         value.var = "flagObservationStatus", fun.aggregate = min,
                         na.rm = TRUE)
setnames(dFlag, colnames(dFlag)[4:6], paste0(colnames(dFlag)[4:6], "_flag"))
dNew = merge(dVal, dFlag)

# For each commodity/country, compute the relevant statistics: quantity of data
# available and magnitude of production.
validObsCnt = function(x){
    sum(x %in% c("", "E", "-"), na.rm = TRUE)
}

stats = dNew[, list(inputObs = validObsCnt(input_flag),
                    productivityObs = validObsCnt(productivity_flag),
                    productionObs = validObsCnt(production_flag),
                    avgProd = mean(production[production_flag %in% c("", "E", "-")],
                                   na.rm = TRUE)),
             by = c("geographicAreaM49", "measuredItemCPC")]
stats[, prodPct := avgProd / sum(avgProd), by = "geographicAreaM49"]

setwd("~/Documents/Github/faoswsProduction/sandbox/")
ggsave("input_data_availability.png",
    ggplot(stats, aes(x = inputObs)) + geom_bar() +
        labs(title = "'Input' Data Availability",
             x = "Official/Semi official/FAO Estimate observations in 1991-2011",
             y = "Number of country/year time series"))
ggsave("productivity_data_availability.png",
    ggplot(stats, aes(x = productivityObs)) + geom_bar() +
        labs(title = "'Productivity' Data Availability",
             x = "Official/Semi official/FAO Estimate observations in 1991-2011",
             y = "Number of country/year time series"))
ggsave("production_data_availability.png",
    ggplot(stats, aes(x = productionObs)) + geom_bar() +
        labs(title = "'Production' Data Availability",
             x = "Official/Semi official/FAO Estimate observations in 1991-2011",
             y = "Number of country/year time series"))
ggsave("production_data_availability_weighted.png",
    ggplot(stats, aes(x = productionObs)) + geom_bar(aes(weight = prodPct)) +
        labs(title = "'Production' Data Availability",
             x = "Official/Semi official/FAO Estimate observations in 1991-2011",
             y = "Number of country/year time series\nWeighted by production percent in country"))

ggsave("production_vs_input_counts_weighted.png",
    ggplot(stats, aes(x = inputObs, y = productionObs, size = prodPct)) +
        geom_point() +
        labs(x = "Official/Semi official/FAO Estimate 'input' obs in 1991-2011",
             y = "Official/Semi official/FAO Estimate 'production' obs in 1991-2011",
             size = "Proportion\nof total\nproduction"))

stats[, prodPctGrp := round(prodPct * 20)/20]
ggplot(stats, aes(x = prodPctGrp, group = prodPctGrp, y = inputObs)) +
    geom_violin()

ggsave("production_vs_input_violin.png",
    ggplot(stats, aes(x = inputObs, group = inputObs, y = productionObs)) +
        geom_violin() +
        labs(x = "Official/Semi official/FAO Estimate 'input' obs in 1991-2011",
             y = "Official/Semi official/FAO Estimate 'production' obs in 1991-2011"))
