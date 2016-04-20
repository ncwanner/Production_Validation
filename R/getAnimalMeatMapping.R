##' This function retrieves the mapping between the parent (animal
##' such as cattle) and child (meat such as meat of cattle).
##'
##' @param R_SWS_SHARE_PATH The path to the shared drive on the SWS.
##'
##' @return The mapping table
##'
##' @export
##'

getAnimalMeatMapping = function(R_SWS_SHARE_PATH, onlyMeatChildren = FALSE,
                                meatPattern = "^211(1|2|7).*"){
    ## fread(paste0(R_SWS_SHARE_PATH,
    ##              "/browningj/production/slaughtered_synchronized.csv"),
    ##       colClasses = "character")

    ## New file which contains the animal group code
    mapping = fread(paste0(R_SWS_SHARE_PATH,
                           "/kao/production/animal_parent_child_mapping.csv"),
                    colClasses = "character")
    if(onlyMeatChildren)
        mapping = mapping[grepl(meatPattern, measuredItemChildCPC), ]
    mapping
}
