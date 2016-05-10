context("Testing function balanceAreaHarvested()")

library(faoswsProduction)

source("buildTestDataset.R")

test_data_rep = copy(test_data)
newMethodFlag = "i"
unitConversion = 1

balanceAreaHarvested(data = test_data_rep,
                     processingParameters = param,
                     newMethodFlag = newMethodFlag,
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
    ## When production available, yield is not missing and not equal
    ## to zero, and area harvested is missing, then area harvested
    ## should be calculated.
    notMissProd = !is.na(test_data[[param$productionValue]])
    notMissYield = (!is.na(test_data[[param$yieldValue]]) &
                         test_data[[param$yieldValue]] != 0)
    missAreaHarvested = is.na(test_data[[param$areaHarvestedValue]])
    filter = notMissProd & notMissYield & missAreaHarvested
    modifiedData = test_data_rep[filter, ]

    ## Check all values are calculated
    expect_equal(sum(is.na(modifiedData[[param$areaHarvestedValue]])), 0)

    ## Check the values are calculated correctly
    ##
    ## NOTE (Michael): We check the identity which is the implied
    ##                 result of the calculation rather than the
    ##                 direct calculation.
    expect_equal(modifiedData[[param$areaHarvestedValue]] *
                 modifiedData[[param$yieldValue]] * unitConversion,
                 modifiedData[[param$productionValue]])

    ## Check all method flags are updated
    expect_true(all(modifiedData[[param$areaHarvestedMethodFlag]] ==
                    newMethodFlag))

    ## Test that the rows that can not be calculated are identical to
    ## the raw data
    expect_identical(test_data[!filter, ], test_data_rep[!filter, ])

})
