isPrimary = function(itemCode, primaryPattern = "^0"){
    grepl(primaryPattern, itemCode)
}
