context("Testing function computeYield()")

library(faoswsProduction)

source("buildTestDataset.R")

test_data_rep = copy(test_data)
newMethodFlag = "i"
unitConversion = 1

computeYield(data = test_data_rep,
             processingParameters = param,
             normalized = FALSE,
             newMethodFlag = newMethodFlag,
             flagTable = faoswsFlagTable,
             unitConversion = unitConversion)

test_that("Function does not modify input", {
    ## Make sure class is the identical
    expect_equal(class(test_data_rep), class(test_data))
    ## Make sure the dimension are the same
    expect_equal(dim(test_data_rep), dim(test_data))
    ## Make sure the type of the columns remain the same
    expect_equal(sapply(test_data_rep, typeof), sapply(test_data, typeof))
})

test_that("Function performs calculation correctly", {
    ## When production available, area harvested is not missing and
    ## not equal to zero, and yield is missing, then yield should be
    ## calculated.
    notMissProd = !is.na(test_data[[param$productionValue]])
    notMissAreaHarvested = !is.na(test_data[[param$areaHarvestedValue]])
    missYield = is.na(test_data[[param$yieldValue]])
    filter = notMissProd & notMissAreaHarvested & missYield
    zeroFilter = test_data[[param$areaHarvestedValue]] != 0
    modifiedData = test_data_rep[zeroFilter & filter, ]


    ## Check all values are calculated
    expect_equal(sum(is.na(modifiedData[[param$yieldValue]])), 0)

    ## Check the values are calculated correctly
    ##
    ## NOTE (Michael): We check the identity which is the implied
    ##                 result of the calculation rather than the
    ##                 direct calculation.
    expect_equal(modifiedData[[param$areaHarvestedValue]] *
                 modifiedData[[param$yieldValue]] * unitConversion,
                 modifiedData[[param$productionValue]])

    ## Check all method flags are updated
    expect_true(all(modifiedData[[param$yieldMethodFlag]] ==
                    newMethodFlag))


    ## Check that all missing yield has missing flag
    expect_true(all(test_data_rep[is.na(get(param$yieldValue)), ][[param$yieldObservationFlag]] == "M"))
    expect_true(all(test_data_rep[is.na(get(param$yieldValue)), ][[param$yieldMethodFlag]] == "u"))

    ## ## Test that the rows that can not be calculated are identical to
    ## ## the raw data
    ## expect_identical(test_data[!filter, ], test_data_rep[!filter, ])

})

