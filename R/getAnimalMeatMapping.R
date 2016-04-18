##' This function retrieves the mapping between the parent (animal
##' such as cattle) and child (meat such as meat of cattle).
##'
##' @return The mapping table
##'
##' @export
##'

getAnimalMeatMapping = function(){
    fread(paste0(R_SWS_SHARE_PATH,
                 "/browningj/production/slaughtered_synchronized.csv"),
          colClasses = "character")
}
