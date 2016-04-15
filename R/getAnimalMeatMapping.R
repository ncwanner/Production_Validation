getAnimalMeatMapping = function(){
    fread(paste0(R_SWS_SHARE_PATH,
                 "/browningj/production/slaughtered_synchronized.csv"),
          colClasses = "character")
}
