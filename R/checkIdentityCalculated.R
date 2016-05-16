##' Function to check whether the triplet (area harvested/production/yield) are
##' calculated whereever possible.
##'
##' @param dataToBeSaved The data.table to be saved back to the SWS.
##' @param areaVar The column name corresponding to the area harvested.
##' @param yieldVar The column name corresponding to the yield.
##' @param prodVar The column name corresponding to produciton.
##' @param returnData logical, whether the data should be returned.
##' @return If the data satisfy the production identity, then the
##'     original data to be tested is returned. Otherwise, an error is
##'     raised.
##' @export
##'


checkIdentityCalculated = function(dataToBeSaved,
                                   areaVar,
                                   yieldVar,
                                   prodVar,
                                   returnData = TRUE){

    stopifnot(all(c(areaVar, yieldVar, prodVar) %in% colnames(dataToBeSaved)))

    for(i in seq(areaVar)){
        ## If the number of NA's is 1, then the identity is not calculated, as
        ## the number can be calculated by the other two non-missing values.
        containOneNA =
            (is.na(dataToBeSaved[[areaVar[i]]]) +
             is.na(dataToBeSaved[[yieldVar[i]]]) +
             is.na(dataToBeSaved[[prodVar[i]]])) == 1

        ## NOTE (Michael): However, yield can be a missing value when area
        ##                 harvested is zero.
        acceptableNACase =
            (dataToBeSaved[[areaVar[i]]] == 0 &
             is.na(dataToBeSaved[[yieldVar[i]]]))

        ## Return the index where identities are not calculated
        identityNotCalculated = setdiff(which(containOneNA), which(acceptableNACase))

        if(length(identityNotCalculated) > 0){
            print(identityNotCalculated)
            stop("Not all entries are calculated")
        }
    }
    if(returnData)
        return(dataToBeSaved)
}
