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

