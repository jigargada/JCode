
library(data.table)
require(readxl)
library(rsgutilr)
library(RODBC)

setwd(paste0("//repsharedfs/share/PricingTeam/Corp PT Only/Broker and National Accounts files/Upload Price Increase Files/Files_From_Field/",format(as.Date(Sys.Date()), "%Y"), "/",format(as.Date(Sys.Date()), "%Y%m"), "/To Be Uploaded"))

#errors <- data.table(read_excel("Errors Upload.xls"))
temp = list.files(pattern="*.xls")
tabls <- list()
keepcols <- c("Index","Comp","Acct","Site","CGrp","Cont Type","QTY","Size","Lifts","Compactor","Charge Code",
              "Type","Method","Charge Rate","New Rate","Effective Date","Tran Code","Rsn Code","NA","UOM","WMT")

#backdate <- backdate[,keepcols,with=FALSE]

for(i in 1:length(temp)) 
{
  #filename[i] <- temp[i]
  tabls[[i]] <- data.table(read_excel(temp[i]))
  setnames(tabls[[i]], old=1:21, new=keepcols)

}

for(i in 1:length(temp)) 
{
  tabls[[i]] <- tabls[[i]][, ':='(Comp = sprintf("%03.f", as.numeric(Comp)),
                                  Acct = as.integer(Acct),
                                  Site = as.integer(Site),
                                  `CGrp` = as.integer(`CGrp`),
                                  `New Rate` = format(round(as.numeric(`New Rate`), 2), nsmall=2),
                                  `Effective Date`= as.Date(`Effective Date`, "%m/%d/%Y"))]
  
  tabls[[i]] <- tabls[[i]][, ':='(`Effective Date`= format(`Effective Date`, "%m/%d/%Y"),
                                  Unique = paste0(Comp,Acct,Site,CGrp,`Charge Code`),
                                  timestamp = Sys.time())]
    
  tabls[[i]] <- tabls[[i]][!is.na(CGrp)]
  
}

for(i in 1:length(temp)) 
{
  if (i==1)
  {
    tabels <- tabls[[1]] 
    
  }  
  else
  {
    tabels <- rbind(tabels, tabls[[i]], use.names = TRUE, fill=TRUE)
    
  }
}

tabels_new <- tabels[,list(Index,Unique, Comp,Acct,Site,CGrp,`Cont Type`,QTY,Size,Lifts,Compactor,`Charge Code`,
                            Type,Method,`Charge Rate`,`New Rate`,`Effective Date`,`Tran Code`,`Rsn Code`,`NA`,UOM,WMT, timestamp)]
setwd("//devpricapp01/d/Jigar/Pricing-Analytics/processes/PI_Rate_Uploads")
rm(tabels)

# channel <- odbcConnect("prodpricsql02_Pricing_Operations")
# sqlSave(channel, tabels_new, PI_Rate_Uploads, append=TRUE,
#         safer=TRUE,nastring='')

sqlInsert(server="prodpricsql02",db="Pricing_Operations",tbl="PI_Rate_Uploads",rdata=tabels_new,windows_auth=TRUE)


####################Backdate_Code##################




Date_effective <- '2017-03-01'

Close_Date <- as.Date(Date_effective)-1

backdate <- data.table(read_excel("//repsharedfs/share/PricingTeam/Corp PT Only/Broker and National Accounts files/Upload Price Increase Files/Files_From_Field/2017/201703/Backdate/Jessica Rice/LAUSD Check 2_J.xls"))

keepcols <- c("Index","Comp","Acct","Site","CGrp","Cont Type","QTY","Size","Lifts","Compactor","Charge Code",
              "Type","Method","Charge Rate","New Rate","Effective Date","Tran Code","Rsn Code","NA","UOM","WMT")

backdate <- backdate[,keepcols,with=FALSE]

backdate_error <- backdate[(is.na(Index) | is.na(Acct))]
backdate <- backdate[!(is.na(Index) | is.na(Acct))]

backdate <- backdate[, ':='(Acct = as.character(Acct),
                                  Site = as.character(Site),
                                  `CGrp` = as.character(`CGrp`),
                            `Charge Rate` = round(as.numeric(`Charge Rate`), digits = 2))]

  
backdate[,`:=`(Comp= sprintf("%03.f", as.numeric(Comp))
               , Site = sprintf("%05.f", as.numeric(Site)),
               `Charge Rate` = sprintf("%.2f", as.numeric(`Charge Rate`)))]

infopro <- backdate[, paste(unique(Comp), sep=",", collapse = "','")]
chargecd <- backdate[, paste(unique(`Charge Code`) , sep=",", collapse = "','")]


query <- paste0("SELECT CCCOMP ,CCACCT ,CCSITE ,CCCTGR ,CCCHCD ,CCCHRT ,CCCHTY ,CCCGMT ,CCTAC FROM STG_IFP_BIPCC_2 where CURRENT_IND = 'Y' and CCBADJ = '0' and CCCOMP in ('", infopro, "') and CCCHCD in ('", chargecd, "')" )

tax_app_code <- data.table(sqlProc(server="prodbirpt", db="DWPSA",
                   sqlFile=query,
                   asText=TRUE, windows_auth = TRUE, as.is = TRUE))

tax_app_code[, `:=`(ccchrt = sprintf("%.2f", as.numeric(ccchrt)))]

setkey(tax_app_code, cccomp, ccacct, ccsite, ccctgr, ccchcd, ccchrt, ccchty, cccgmt)

setkey(backdate, Comp, Acct, Site, CGrp, `Charge Code`, `Charge Rate`, Type, Method)

backdate_tax_app <- merge(backdate, tax_app_code, by.x= c("Comp", "Acct", "Site", "CGrp", "Charge Code", "Charge Rate", "Type", "Method"),
                          by.y= c("cccomp", "ccacct", "ccsite", "ccctgr", "ccchcd", "ccchrt", "ccchty", "cccgmt"), all.x = TRUE)

backdate_tax_app <- backdate_tax_app[, `:=`(`Charge Rate` = round(as.numeric(`Charge Rate`), digits = 2),
                                            `New Rate` = round(as.numeric(`New Rate`), digits = 2),
                                            Close_Date = Close_Date)]

write.table(backdate_tax_app, file = "//repsharedfs/share/PricingTeam/Corp PT Only/Broker and National Accounts files/Upload Price Increase Files/Files_From_Field/2017/201703/Backdate/Jessica Rice/LAUSD Check 2_U.csv", 
            append = FALSE, sep = ",", eol = "\n", na = "", dec = ".", row.names = FALSE, col.names = TRUE)
