##' Get Yield Formula
##' 
##' This function provides the element codes (for production, yield, and output)
##' and conversion factor which correspond to the particular item code of
##' interest.
##' 
##' @param itemCode The cpc code providing the commodity of interest.  This 
##' should be a character string.
##'   
##' @return A data.table object providing the relevant element codes.  In some
##' cases, there may be multiple rows, as it is possible to have multiple
##' elements which hold production, yield, and output data.
##' 

getYieldFormula = function(itemCode){
    itemData = GetCodeList(domain = "agriculture", dataset = "agriculture",
                           dimension = "measuredItemCPC", codes = itemCode)
    itemData = itemData[!is.na(type), ]
    if(nrow(itemData) == 0)
        stop("No valid data to process!  Maybe the item code isn't in the ",
             "database?")
    uniqueItemTypes = unique(itemData$type)
    condition =
        paste0("WHERE item_type IN (",
               paste(paste0("'", as.character(uniqueItemTypes), "'"),
                      collapse = ", "), ")")
    yieldFormula =
        GetTableData(schemaName = "ess",
                     tableName = "item_type_yield_elements",
                     whereClause = condition)
    yieldFormula = merge.data.frame(itemData, yieldFormula,
                                    by.x = "type", by.y = "item_type")
    yieldFormula = yieldFormula[, c("code", "element_31", "element_41",
                                    "element_51", "factor")]
    yieldFormula = data.table(yieldFormula)
    setnames(yieldFormula,
             old = c("code", "element_31", "element_41",
                 "element_51", "factor"),
             new = c(itemVar, "input", "productivity",
                 "output", "unitConversion")
             )
    yieldFormula
}
