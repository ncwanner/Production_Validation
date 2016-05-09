removeIndigenousBiologicalMeat = function(formula){
    ## FROM (JOSH): Indigenous and biological eat should not be imputed,
    ##              this should be fixed at the database level where they
    ##              are seperate items rather then different elements.
    formula[nchar(input) == 4, ]
}
