##' Clean Production Data
##' 
##' This function does some basic cleaning of the dataset, such as removing 
##' unneeded columns and checking if there is any missing data to impute.
##' 
##' @param datasets An object as produced by getProductionData.
##' @param i The row number of formulaTuples which we are currently working 
##'   with.  At the time of writing this function, some commodities could have 
##'   multiple different imputations (biological and indigineous meat is stored 
##'   under the code for meat).  This is likely to be changed eventually, and 
##'   this argument would then no longer be needed.
##' @param maxYear The final values in a time series can be blank cells, and this 
##'   algorithm should provide estimates for such missing values.  As such, this
##'   function creates 0Mu values up to the maximum year required.  This maximum
##'   year is determined by either the maximum of the year variable in datasets 
##'   (if this parameter is NULL) or by this parameter.  Note that setting a
##'   maximum year that is smaller than the maximum year in the dataset will do
##'   nothing.
##'   
##' @return No object is returned, but the passed dataset is modified in place 
##'   via data.table.
##'   

cleanData = function(datasets, i, maxYear = NULL){
    
    codes = datasets$formulaTuples[i, c("input", "productivity",
                                    "output"), with = FALSE]
    allCols = c("measuredItemCPC", "geographicAreaM49", "timePointYears",
                sapply(codes, function(code){
                    paste0(c("Value", "flagObservationStatus", "flagMethod"),
                           "_measuredElement_", code)
                })
    )
    badCols = setdiff(colnames(datasets$query), allCols)
    if(length(badCols) >= 1){ # don't run if 0 so as to avoid warning
        datasets$query[, badCols := NULL, with = FALSE]
    }

    valueCols = paste0(datasets$prefixTuples$valuePrefix,
                       datasets$formulaTuples[i, c("productivity", "output"),
                                              with = FALSE])
    if(all(is.na(datasets$query[[valueCols[1]]])) &
       all(is.na(datasets$query[[valueCols[2]]]))){
        stop("No non-missing data!")
    }
    
    ## Some rows may be missing entirely, and thus we may fail to impute
    ## for those years/countries/commodities if we don't add rows with 
    ## missing data.  However, if the last observed value was a 0 we
    ## should assume that commodity has remained 0.  0Mu, or missing,
    ## should not be considered for this adjustment.
    countryCommodity = unique(datasets$query[, c(areaVar, itemVar),
                                             with = FALSE])
    ## To merge two data.tables, we need a key column.  Create a dummy
    ## one to do the merge.
    countryCommodity[, mergeKey := 1]
    years = unique(datasets$query[, get(yearVar)])
    if(!is.null(maxYear))
        years = union(years, years[1]:maxYear)
    year = data.table(years)
    setnames(year, yearVar)
    year[, mergeKey := 1]
    fullData = merge(countryCommodity, year, by = "mergeKey",
                     allow.cartesian = TRUE)
    fullData[, mergeKey := NULL]
    ## Merge fullData back to datasets$query.  If a record was missing
    ## in datasets$query, it will now exist with NA values/flags.
    datasets$query = merge(datasets$query, fullData,
                           by = c(itemVar, areaVar, yearVar), all.y = TRUE)
    ## Replace NA/NA/NA with 0/E/i or 0/M/u
    ignoreZeroSeries(datasets$query, missingObsFlag = "M", missingMetFlag = "u",
                     firstImputeYear = 2010)
    return(datasets)
}
