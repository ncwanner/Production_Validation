##' Function to check whether the triplet (area harvested/production/yield) are
##' calculated whereever possible.
##'
##' @param dataToBeSaved The data.table to be saved back to the SWS.
##' @param areaVar The column name corresponding to the area harvested.
##' @param yieldVar The column name corresponding to the yield.
##' @param prodVar The column name corresponding to produciton.
##' @param conversion The conversion factor for calculating production.
##'
##' @return If the data satisfy the production identity, then the
##'     original data to be tested is returned. Otherwise, an error is
##'     raised.
##' @export
##'


checkIdentityCalculated = function(dataToBeSaved,
                                   areaVar,
                                   yieldVar,
                                   prodVar){

    stopifnot(all(c(areaVar, yieldVar, prodVar) %in% colnames(dataToBeSaved)))

    for(i in seq(areaVar)){
        number_of_na =
            is.na(dataToBeSaved[[areaVar[i]]]) +
            is.na(dataToBeSaved[[yieldVar[i]]]) +
            is.na(dataToBeSaved[[prodVar[i]]])

        if(any(number_of_na == 1)){
            print(which(number_of_na == 1))
            stop("Not all entries are calculated")
        }
    }
    dataToBeSaved
}
