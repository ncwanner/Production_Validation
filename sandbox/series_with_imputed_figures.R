## load the library
library(faosws)
library(faoswsUtil)
library(faoswsFlag)
library(faoswsImputation)
library(data.table)
library(splines)
library(lme4)

SetClientFiles("~/R certificate files/Production/")
GetTestEnvironment(
    baseUrl = "https://hqlprswsas1.hq.un.fao.org:8181/sws",
    token = "ad797167-20aa-4ff4-b679-461c96e0da79"
)
setwd("~/Documents/Github/faoswsProduction/sandbox/")

areaVar = "geographicAreaM49"
yearVar = "timePointYears"
itemVar = "measuredItemCPC"
elementVar = "measuredElement"
runParallel = FALSE


years = 1991:2014
apiDirectory = "~/Documents/Github/faoswsProduction/R/"

## Source local scripts for this local test
for(file in dir(apiDirectory, full.names = T))
    source(file)

fullKey = getMainKey(years = years)

## Get main dataset
data = GetData(fullKey)
GetCodeList("agriculture", "aproduction", "measuredItemCPC")[grepl("ffals ", description), ]

## Get series for animals
animalCodes = fread("/media/hqlprsws2_prod/browningj/production/slaughtered_synchronized.csv")
animalKey = fullKey
animalKey@dimensions$measuredItemCPC@keys = unique(animalCodes$measuredItemParentCPC)
animalKey@dimensions$measuredElement@keys = c(unique(animalCodes$measuredElementParent),
                                              "5111", "5112")
animalData = GetData(animalKey)
## Convert to children series
animalData = merge(animalData, animalCodes, by.x = "measuredItemCPC",
                   by.y = "measuredItemParentCPC", allow.cartesian = TRUE)
animalData[, measuredItemCPC := measuredItemChildCPC]
animalData[, c("Item Name", "measuredElementParent", "measuredItemChildCPC",
               "Child Item Name", "measuredElementChild") := NULL]
data = rbind(data, animalData)

## Data summaries
data = data[flagObservationStatus != "M", ]
data[, imputed := flagObservationStatus %in% c("I", "E")]
stats = data[, list(
        totalSeries = length(unique(measuredElement)),
        nonImputedSeries = length(unique(measuredElement[!imputed])),
        totalRecords = .N,
        nonImputedRecords = sum(!imputed),
        totalYears = length(unique(timePointYears)),
        nonImputedYears = length(unique(timePointYears[!imputed]))),
    by = c("geographicAreaM49", "measuredItemCPC")]
badSeries = stats[nonImputedRecords == 0, list(geographicAreaM49, measuredItemCPC)]
badData = merge(data, badSeries, by = colnames(badSeries))
badData[, imputed := FALSE]
write.csv(badData, file = "all_series_with_only_imputations.csv", row.names = FALSE)
statsToSave = copy(stats)
statsToSave[, c("totalSeries", "nonImputedSeries", "totalRecords",
                "nonImputedRecords", "totalYears") := NULL]
setnames(statsToSave, "nonImputedYears", "years_with_data")
write.csv(statsToSave, file = "series_summary_by_country_and_item.csv", row.names = FALSE)

countryStats = statsToSave[, .N, by = c("geographicAreaM49", "years_with_data")]
# countryStats = dcast(data = countryStats, formula = geographicAreaM49 ~ years_with_data,
#                      value.var = "N", fun.agg = sum)
countryStats = countryStats[order(years_with_data), ]
countryStats[, cumulativeCount := cumsum(N), by = geographicAreaM49]
countryStats[, cumulativePct := cumsum(N)/sum(N), by = geographicAreaM49]
ggsave("data_availability_by_country.png",
    ggplot(countryStats, aes(x = years_with_data, y = cumulativePct,
                             group = geographicAreaM49)) +
        geom_line(alpha = .2) +
        labs(x = "Years of available data", y = "Cumulative % of time series",
             title = "Data availability by country") +
        ylim(c(0, 1))
)
ggsave("data_availability_by_country_faceted.png",
    ggplot(countryStats, aes(x = years_with_data, y = cumulativePct,
                             group = geographicAreaM49)) +
        geom_line() + facet_wrap( ~ geographicAreaM49) +
        labs(x = "Years of available data", y = "Cumulative % of time series",
             title = "Data availability by country") +
        ylim(c(0, 1)),
    width = 20, height = 20)

itemStats = statsToSave[, .N, by = c("measuredItemCPC", "years_with_data")]
# countryStats = dcast(data = countryStats, formula = geographicAreaM49 ~ years_with_data,
#                      value.var = "N", fun.agg = sum)
itemStats = itemStats[order(years_with_data), ]
itemStats[, cumulativeCount := cumsum(N), by = measuredItemCPC]
itemStats[, cumulativePct := cumsum(N)/sum(N), by = measuredItemCPC]
ggsave("data_availability_by_commodity.png",
    ggplot(itemStats, aes(x = years_with_data, y = cumulativePct,
                             group = measuredItemCPC)) +
        geom_line(alpha = .2) +
        labs(x = "Years of available data", y = "Cumulative % of time series",
             title = "Data availability by commodity") +
        ylim(c(0, 1))
)
ggsave("data_availability_by_commodity_faceted.png",
    ggplot(itemStats, aes(x = years_with_data, y = cumulativePct)) +
        geom_line() + facet_wrap( ~ measuredItemCPC) +
        labs(x = "Years of available data", y = "Cumulative % of time series",
             title = "Data availability by commodity") +
        ylim(c(0, 1)),
    width = 20, height = 20)

itemTypeMap = GetCodeList("agriculture", "aproduction", "measuredItemCPC")
statsToSave = merge(statsToSave, itemTypeMap[, list(code, type)],
                    by.x = "measuredItemCPC", by.y = "code", all.x = TRUE)
itemTypeStats = statsToSave[, .N, by = c("type", "years_with_data")]
# countryStats = dcast(data = countryStats, formula = geographicAreaM49 ~ years_with_data,
#                      value.var = "N", fun.agg = sum)
itemTypeStats = itemTypeStats[order(years_with_data), ]
itemTypeStats[, cumulativeCount := cumsum(N), by = type]
itemTypeStats[, cumulativePct := cumsum(N)/sum(N), by = type]
ggsave("data_availability_by_item_type.png",
    ggplot(itemTypeStats, aes(x = years_with_data, y = cumulativePct,
                             group = type)) +
        geom_line(alpha = .2) +
        labs(x = "Years of available data", y = "Cumulative % of time series",
             title = "Data availability by item type") +
        ylim(c(0, 1))
)
ggsave("data_availability_by_item_type_faceted.png",
    ggplot(itemTypeStats, aes(x = years_with_data, y = cumulativePct)) +
        geom_line() + facet_wrap( ~ type) +
        labs(x = "Years of available data", y = "Cumulative % of time series",
             title = "Data availability by item type") +
        ylim(c(0, 1)),
    width = 20, height = 20)


totalStats = statsToSave[, .N, by = c("years_with_data")]
# countryStats = dcast(data = countryStats, formula = geographicAreaM49 ~ years_with_data,
#                      value.var = "N", fun.agg = sum)
totalStats = totalStats[order(years_with_data), ]
totalStats[, cumulativeCount := cumsum(N)]
totalStats[, cumulativePct := cumsum(N)/sum(N)]
ggsave("data_availability_total.png",
    ggplot(totalStats, aes(x = years_with_data, y = cumulativePct)) +
        geom_line() +
        labs(x = "Years of available data", y = "Cumulative % of time series",
             title = "Data availability") +
        ylim(c(0, 1))
)