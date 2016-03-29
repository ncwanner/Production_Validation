context("Testing function imputeProductionDomain()")

library(faoswsProduction)

source("buildTestDataset.R")

test_data_rep = copy(test_data)
newObservationFlag = "I"
newMethodFlag = "i"
unitConversion = 1
yieldImputationParameter = defaultImputationParameters(variable = "yield")
prodImputationParameter = defaultImputationParameters(variable = "production")

imputed = imputeProductionDomain(data = test_data_rep,
                                 processingParameters = param,
                                 yieldImputationParameter =
                                     yieldImputationParameter,
                                 productionImputationParameters =
                                     prodImputationParameter,
                                 unitConversion = unitConversion)


test_that("Function does not modify input", {
    ## Make sure class is the identical
    expect_equal(class(imputed), class(test_data_rep))
    ## Make sure the original variables are preserved
    expect_true(all(colnames(test_data_rep) %in% colnames(imputed)))
    ## Make sure the type of the columns remain the same
    expect_equal(sapply(imputed[, colnames(test_data_rep), with = FALSE], typeof),
                 sapply(test_data_rep, typeof))
})

test_that("Function imputes all missing values", {
    expect_true(!any(is.na(imputed[[param$productionValue]])))
    expect_true(!any(is.na(imputed[[param$areaHarvestedValue]])))
    expect_true(!any(is.na(imputed[[param$yieldValue]])))
})
