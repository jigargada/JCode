library(data.table)
library(RODBC)
library(xlsx)
library(stringr)
library(rsgutilr)

mkt <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Email Retention Cust_Data (for Marketing dept)_V4.csv", header = TRUE))
takers_mn <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 1 - July 2016_MN.csv", header=TRUE))
takers_nj <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 1 - July 2016_NJ.csv", header=TRUE))
takers_littlerock <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 1 - July 2016_Little_Rock.csv", header=TRUE))
declined <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 1 - July 2016_Declined.csv", header=TRUE))
takers<- rbind(takers_mn, takers_nj, takers_littlerock, declined)

setkey(mkt, Account.Number)
setkey(takers, Account.Number)
takers <- mkt[takers]

recent_changes <- data.table(sqlProc(server="prodbirpt", db="DWCORE",
                                     sqlFile="SELECT convert(int, COMPANY_NUMBER) as COMPANY_NUMBER
                                  ,convert(int, [ACCOUNT_NUMBER]) as [ACCOUNT_NUMBER]
                                     ,convert(int, [SITE]) as [SITE]
                                     ,convert(int, CONTAINER_GROUP) as CONTAINER_GROUP
                                     FROM [DWPSA].[dbo].[STG_IFP_BIPCCR]
                                     where Company_Number in ('894', '899', '923', '858', '282', '320', '865')
                                     and EFFECTIVE_DATE > 20160707 
                                     and CLOSE_DATE between 20160707 and CONVERT(varchar(8), GETDATE(), 112)",
                                  asText=TRUE,windows_auth = TRUE, as.is = TRUE))

recent_changes <- recent_changes[,`:=`(company_number= sprintf("%03.f", as.numeric(company_number)),
                         account_number = sprintf("%07.f", as.numeric(account_number)))]
recent_changes[,`:=`(account_number = paste0(company_number, "-", account_number ))]

setkey(mkt, Account.Number)
setkey(recent_changes, account_number)

recent_changes <- mkt[recent_changes]
recent_changes <- recent_changes[!is.na(Email)]
recent_changes <- recent_changes[, list(Account.Number, Email)]
recent_changes <- unique(recent_changes, by = c("Account.Number", "Email"))

takers <- takers[, list(Account.Number, Email)]
exclusion <- rbind(takers, recent_changes)
exclusion <- exclusion[!duplicated(exclusion),]

write.table(exclusion, paste0("//prodpricapp02/d/Analytics/Resi Email Retention/Exclusion.csv"), sep=",",na="",row.names=FALSE)
