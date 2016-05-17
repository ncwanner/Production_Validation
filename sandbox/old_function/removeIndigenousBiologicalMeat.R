##' This function removes the entry which corresponds to indigenous and
##' biological meats.
##'
##' @param formula The object returned by the \code{getYieldFormula}
##'
##' @return The filtered formula without biological and indigenous treatment.
##' @export
##'

removeIndigenousBiologicalMeat = function(formula){
    ## FROM (JOSH): Indigenous and biological eat should not be imputed,
    ##              this should be fixed at the database level where they
    ##              are seperate items rather then different elements.
    warning("Indigenous and Biological Meat requires special treatment!")
    formula[nchar(input) == 4, ]
}
