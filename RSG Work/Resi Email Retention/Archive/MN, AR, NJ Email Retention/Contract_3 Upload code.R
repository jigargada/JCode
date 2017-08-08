library(rsgutilr)
library(lubridate)
library (stringr)
library(data.table)

wave_date <- 0510

mkt <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Email Retention Cust_Data (for Marketing dept)_V4.csv", header = TRUE))
load("//prodpricapp02/d/Analytics/Resi Email Retention/rate_upload.robj")

takers_mn <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 3 - July 2016_MN.csv", header=TRUE))
takers_nj <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 3 - July 2016_NJ.csv", header=TRUE))
takers_littlerock <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 3 - July 2016_Little_Rock.csv", header=TRUE))

takers<- rbind(takers_mn, takers_nj, takers_littlerock)


setkey(mkt, Account.Number)
setkey(takers, Account.Number)
mkt <- mkt[takers]

mkt <- mkt[, `:=` (Account.Number = paste0(substr(Account.Number, 1,3), substr(Account.Number, 5,11)))]
setkey(mkt, Account.Number)
setkey(rate_upload, acct_nbr)

rate_upload <- rate_upload[mkt]


# x <- baseConvert(as.period(as.interval(as.POSIXct(paste0(substr(str_match("20160721000000","[[:digit:]]{14}")[1],1,4),
# "0101000000"),format = "%Y%m%d%H%M%S"), +as.POSIXct(str_match("20160914000000","[[:digit:]]{14}")[1],
#  format="%Y%m%d%H%M%S")),unit="minutes")@minute,36)

mkt[, `:=` (indexa= seq(from =1, to =nrow(mkt), by=1))]

mkt[, `:=` (temp = str_pad(as.character(baseConvert(indexa,36,10)), width=4, side="left", pad="0"))]

mkt[, `:=` (contract_number=paste0("RM", wave_date, temp))] 
dt <- copy(mkt)

dt <- dt[, `:=`(Date.Signed = as.character(gsub("\\.", "/", Date.Signed)))]

dt <- dt[, `:=` (Term = "36")]
dt <- dt[, `:=` (Expiration_Date = as.Date(Date.Signed, "%m/%d/%Y") + (365*3),
                 Date.Signed = as.Date(Date.Signed, "%m/%d/%Y"))]

dt_1 <- dt[, list(InfoPro.Division.Number, Account.Number, contract_number, Date.Signed, Term, Expiration_Date )]

setkey(dt_1, Account.Number)

setkey(rate_upload, acct_nbr)

dt_1 <- rate_upload[dt_1]

UPload_this <- dt_1[, list(infopro_div_nbr, acct_nbr, site_nbr, contract_number, Term, Date.Signed, Expiration_Date)]
UPload_this <- UPload_this[, `:=` (acct_nbr = as.numeric(substr(acct_nbr, 4,10)))]
UPload_this <- UPload_this[, unique(UPload_this, by = c("infopro_div_nbr","acct_nbr", "site_nbr" ))]

write.table(UPload_this, paste0("//prodpricapp02/d/Analytics/Resi Email Retention/Contract_Upload_NJ_AR_MN_wave_3.csv"),
            sep=",",na="",row.names=FALSE)

# save.image("//prodpricapp02/d/Analytics/Resi Email Retention/Contracts__wave.Rdata")
