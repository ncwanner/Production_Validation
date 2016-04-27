##' Function to check whether the data will over write protected data.
##'
##' Certain data in the Statistical Working System should not be over
##' written by algorithms. Namely, official and semi-official
##' values. This function takes the data to be saved in the arguement
##' and pulls the exact same set of data from the data base, then
##' checks whether the matching set contains official or semi official
##' values.
##'
##' @param dataToBeSaved The data.table object containing data to be
##'     saved back to the database. The current implementation only
##'     accepts data in the normalised form.
##' @param domain The domain name in the SWS where the data will be
##'     saved.
##' @param dataset The dataset name in the SWS where the data will be
##'     saved.
##' @param areaVar The column name corresponding to the geographic
##'     area.
##' @param itemVar The column name corresponding to the commodity
##'     item.
##' @param elementVar The column name corresponding to the measured
##'     element.
##' @param yearVar The column name corresponding to the year.
##' @param flagObsVar The column name corresponding to the observation
##'     status flag.
##' @param protectedFlag The set of flag values which corresponds to
##'     values that should not be over written.
##' 
##' @return If the data set passes the test, then the original data
##'     will be returned. Otherwise an error will be raised.
##'
##' @export
##'

checkProtectedData = function(dataToBeSaved,
                              domain = "agriculture",
                              dataset = "aproduction",
                              areaVar = "geographicAreaM49",
                              itemVar = "measuredItemCPC",
                              elementVar = "measuredElement",
                              yearVar = "timePointYears",
                              flagObsVar = "flagObservationStatus",
                              protectedFlag = c("", "*")){
    
    
    dataToBeSavedCopy = copy(dataToBeSaved)
    setkeyv(dataToBeSavedCopy, col = c(areaVar, itemVar, elementVar, yearVar))

    
    if(NROW(dataToBeSavedCopy) > 0){
        newKey = DatasetKey(
            domain = domain,
            dataset = dataset,
            dimensions = list(
                Dimension(name = areaVar,
                          keys = unique(dataToBeSavedCopy[[areaVar]])),
                Dimension(name = itemVar,
                          keys = unique(dataToBeSavedCopy[[itemVar]])),
                Dimension(name = elementVar,
                          keys = unique(dataToBeSavedCopy[[elementVar]])),
                Dimension(name = yearVar,
                          keys = unique(dataToBeSavedCopy[[yearVar]]))
            )
        )
        dbData = GetData(newKey)
        setkeyv(dbData, col = c(areaVar, itemVar, elementVar, yearVar))
        matchSet = dbData[dataToBeSavedCopy, ]
        protectedData = matchSet[matchSet[[flagObsVar]] %in% protectedFlag, ]
        if(NROW(protectedData) > 0)
            stop("Protected Data being over written!")
    } else {
        warning("Data to be saved contain no entry")
    }
    dataToBeSaved
}
