

message("Step 0: Setup")

##' Load the libraries
suppressMessages({
    library(data.table)
    library(faosws)
    library(faoswsFlag)
    library(faoswsUtil)
    library(faoswsImputation)
    library(faoswsProduction)
    library(faoswsProcessing)
    library(faoswsEnsure)
    library(magrittr)
    library(dplyr)
})

##' Get the shared path
R_SWS_SHARE_PATH = Sys.getenv("R_SWS_SHARE_PATH")

if(CheckDebug()){
    
    library(faoswsModules)
    SETTINGS = ReadSettings("sws.yml")
    
    ## If you're not on the system, your settings will overwrite any others
    R_SWS_SHARE_PATH = SETTINGS[["share"]]
    
    ## Define where your certificates are stored
    SetClientFiles(SETTINGS[["certdir"]])
    
    ## Get session information from SWS. Token must be obtained from web interface
    GetTestEnvironment(baseUrl = SETTINGS[["server"]],
                       token = SETTINGS[["token"]])
    
}


##' Load and check the computation parameters
imputationSelection = swsContext.computationParams$imputation_selection
if(!imputationSelection %in% c("session", "all"))
    stop("Incorrect imputation selection specified")


##' Get data configuration and session
sessionKey = swsContext.datasets[[1]]
datasetConfig = GetDatasetConfig(domainCode = sessionKey@domain,
                                 datasetCode = sessionKey@dataset)

##' Build processing parameters
processingParameters =
    productionProcessingParameters(datasetConfig = datasetConfig)

##' Obtain the complete imputation key
completeImputationKey = getCompleteImputationKey("production")

completeImputationKey@dimensions$measuredItemCPC@keys=c("0231","02151")
completeImputationKey@dimensions$measuredElement@keys=c("5313","5112")


toPlot=GetData(completeImputationKey)



library(faosws)
library(faoswsUtil)
library(data.table)
library(igraph)
library(faoswsBalancing)
library(faoswsStandardization)
library(dplyr)
library(MASS) 
library(lattice)
library(reshape2)
library(forecast)
library(tidyr)

onlyProtected=TRUE

countryName=nameData("agriculture", "aproduction", toPlot[,.(geographicAreaM49)])
countryName=unique(countryName)


elementLabel=nameData("agriculture", "aproduction", toPlot[,.(measuredElement)])
elementLabel=unique(elementLabel)


syncedDataLabel=merge(toPlot,countryName, by="geographicAreaM49",all.x=TRUE)
syncedDataLabel=merge(syncedDataLabel,elementLabel, by="measuredElement",all.x=TRUE)
syncedDataLabel[, timePointYears:=as.numeric(timePointYears)]
syncedDataLabel[, measuredItemCPC:="02111"]




if(onlyProtected){
    
    syncedDataLabel=removeNonProtectedFlag(syncedDataLabel)
    pdf(paste0("C:/Users/Rosa/Desktop/NEW LIVESTOCK MODULE/Chart/EggHenPROTECTED.pdf"),
        paper = "a4",width=9, height=15)  
    
    
}else{
    
    pdf(paste0("C:/Users/Rosa/Desktop/NEW LIVESTOCK MODULE/Chart/EggHen.pdf"),
        paper = "a4",width=9, height=15)  
    
}

geo=sort(unique(syncedDataLabel[, geographicAreaM49]))
pnp=list()



pags = 12
nump <- seq(1,length(geo),pags)
if(length(geo)/pags==length(geo)%/%pags){
    lgp=length(geo)/pags}else{
        lgp=length(geo)%/%pags+1
    }

for(i in 1:lgp){
    if(!is.na(nump[i+1])){
        pnp[[i]] <-  ggplot(syncedDataLabel[geographicAreaM49 %in% geo[nump[i]:nump[i+1]-1]], aes(x=timePointYears, y=Value)) + 
            geom_line(aes(linetype=measuredElement_description,color=measuredElement_description,size=measuredElement_description)) +
            scale_x_continuous(breaks=1990:2016) +
            scale_linetype_manual(values=c("solid","longdash","dotdash","solid","solid")) +
            scale_colour_manual(values = c("red","blue","red","yellow","green")) +
            scale_size_manual(values=c(0.3,0.5,0.3,0.3,0.3)) +
            theme(axis.title =element_text(size=5),
                  axis.text.y = element_text(size=5),
                  axis.text.x = element_text(size=4,angle = 50, hjust = 1),
                  legend.text = element_text(size=6),
                  strip.text.x = element_text(size = 7),
                  legend.position = "top",
                  panel.grid.major = element_line(colour = "grey80", linetype = "longdash",size= 0.2 ), 
                  panel.grid.minor = element_line(colour="white",size=0), 
                  panel.background = element_rect(fill="white")) +
            facet_wrap(~geographicAreaM49_description, ncol = 3,scales = "free")
        print(pnp[[i]])
        
    }else{
        if(!is.na(nump[i])){
            pnp[[i]] <-  ggplot(syncedDataLabel[geographicAreaM49 %in% geo[nump[i]:length(geo)]], aes(x=timePointYears, y=Value)) + 
                geom_line(aes(linetype=measuredElement_description,color=measuredElement_description,size=measuredElement_description)) + 
                scale_x_continuous(breaks=1990:2016) +
                scale_linetype_manual(values=c("solid","longdash","dotdash","solid","solid")) +
                scale_colour_manual(values = c("red","blue","red","yellow","green")) +
                scale_size_manual(values=c(0.3,0.5,0.3,0.3,0.3)) +
                theme(axis.title =element_text(size=5),
                      axis.text.y = element_text(size=5),
                      axis.text.x = element_text(size=4,angle = 50, hjust = 1),
                      legend.text = element_text(size=6),
                      strip.text.x = element_text(size = 7),
                      legend.position = "top",
                      panel.grid.major = element_line(colour = "grey80", linetype = "longdash",size= 0.2 ), 
                      panel.grid.minor = element_line(colour="white",size=0), 
                      panel.background = element_rect(fill="white")) +
                facet_wrap(~geographicAreaM49_description, ncol = 3,scales = "free") 
            print(pnp[[i]])
            
        }}}


dev.off()
