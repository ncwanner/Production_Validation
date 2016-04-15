checkProtectedData = function(dataToBeSaved,
                              domain = "agriculture",
                              dataset = "aproduction",
                              areaVar = "geographicAreaM49",
                              yearVar = "timePointYears",
                              itemVar = "measuredItemCPC",
                              elementVar = "measuredElement",
                              flagObsVar = "flagObservationStatus",
                              flagMethodVar = "flagMethod",
                              protectedFlag = c("", "*"),
                              p = defaultProcessingParameters()){
    if(NROW(dataToBeSaved) > 0){
        newKey = DatasetKey(
            domain = domain,
            dataset = dataset,
            dimensions = list(
                Dimension(name = areaVar,
                          keys = unique(dataToBeSaved[[areaVar]])),
                Dimension(name = itemVar,
                          keys = unique(dataToBeSaved[[itemVar]])),
                Dimension(name = elementVar,
                          keys = unique(dataToBeSaved[[elementVar]])),
                Dimension(name = yearVar,
                          keys = unique(dataToBeSaved[[yearVar]]))
            )
        )

        dbData = GetData(newKey)

        protectedData = dbData[.SD[[flagObsVar]] %in% protectedFlag, ]
        if(NROW(protectedData) > 0)
            stop("Protected Data being over written!")
    } else {
        warning("Data to be saved contain no entry")
    }
    dataToBeSaved
}
