library(faoswsProduction)

param = defaultProcessingParameters()


## NOTE (Michael): Need to add in measuredItemCPC
##
## NOTE (Michael): Maybe should construct the data based on the
##                 parameters.
test_data = unique(okrapd[, .(geographicAreaM49, timePointYears)])
nobs = NROW(test_data)

test_data[, Value_measuredElement_5312 :=
              ifelse(sample(c(0, 1), size = nobs, prob = c(0.8, 0.2),
                            replace = TRUE) == 0,
                     runif(n = nobs, min = 0, max = 1e7), NA)]
test_data[, flagObservationStatus_measuredElement_5312 :=
              ifelse(is.na(Value_measuredElement_5312),
                     "M", sample(c("T", "E", "F", ""), 1))]

test_data[, Value_measuredElement_5416 :=
              ifelse(sample(c(0, 1), size = nobs, prob = c(0.8, 0.2),
                            replace = TRUE) == 0,
                     runif(n = nobs, min = 0, max = 1e7), NA)]
test_data[, flagObservationStatus_measuredElement_5416 :=
              ifelse(is.na(Value_measuredElement_5416),
                     "M", sample(c("T", "E", "F", ""), 1))]

test_data[, Value_measuredElement_5510 :=
              ifelse(sample(c(0, 1), size = nobs, prob = c(0.8, 0.2),
                            replace = TRUE) == 0,
                     runif(n = nobs, min = 0, max = 1e7), NA)]
test_data[, flagObservationStatus_measuredElement_5510 :=
              ifelse(is.na(Value_measuredElement_5510),
                     "M", sample(c("T", "E", "F", ""), 1))]

test_data[, c("flagMethod_measuredElement_5510",
            "flagMethod_measuredElement_5416",
            "flagMethod_measuredElement_5312") := 'n']


test_data_rep = copy(test_data)
newObservationFlag = "I"
newMethodFlag = "i"
unitConversion = 1
balanceAreaHarvested(data = test_data_rep,
                     processingParameters = param,
                     newObservationFlag = newObservationFlag,
                     newMethodFlag = newMethodFlag,
                     unitConversion = unitConverstion)

test_that("Function does not modify input", {
    ## Make sure class is the identical
    expect_equal(class(test_data_rep), class(test_data))
    ## Make sure the dimension are the same
    expect_equal(dim(test_data_rep), dim(test_data))
    ## Make sure the type of the columns remain the same
    expect_equal(sapply(test_data_rep, typeof), sapply(test_data, typeof))
})

test_that("Function performs calculation correctly", {
    ## When production available, yield is not missing and not
    ## equal to zero, then area harvested should be calculated.
    notMissProd = which(!is.na(test_data[[param$productionValue]]))
    notMissYield = which(!is.na(test_data[[param$yieldValue]]) &
                         test_data[[param$yieldValue]] != 0)
    
    
    canCalculateArea = intersect(notMissProd, notMissYield)
    calculatedAreaRows =
        test_data_rep[canCalculateArea, ]

    ## Check all values are calculated
    expect_equal(sum(is.na(calculatedAreaRows[[param$areaHarvestedValue]])), 0)

    ## Check all flags are updated
    expect_true(all(calculatedAreaRows[[param$areaHarvestedObservationFlag]] == newObservationFlag))

    cannotCalculateArea = setdiff(1:nobs, canCalculateArea)

    ## Test that the rows that can not be calculated are identical
    expect_identical(test_data[cannotCalculateArea, ],
                     test_data_rep[cannotCalculateArea])


    ## Test whether the values are calculated correctly. Need to
    ## account for roundings!
    
})
