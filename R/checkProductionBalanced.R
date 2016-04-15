checkProductionBalanced = function(data, areaVar, yieldVar, prodVar, conversion){
    productionDifference =
        abs(data[[areaVar]] * data[[yieldVar]] - data[[prodVar]] * conversion)
    ## NOTE (Michael): Test whether the difference is below 1, this is
    ##                 accounting for rounding error.
    if(!all(na.omit(productionDifference < 1))){
        stop("Production is not balanced, the A * Y = P identity is not satisfied")
    }
}
