
library(data.table)
library(RODBC)
library(zoo)
library(stringr)
library(rsgutilr)
library(lubridate)

load("//prodpricapp02/d/Analytics/Resi Email Retention/rate_upload.robj")
mkt <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Email Retention Cust_Data (for Marketing dept)_V4.csv", header = TRUE))
rate_upload <- rate_upload[, `:=`(acct_nbr = paste0(substr(acct_nbr, 1,3), "-", substr(acct_nbr,4,10)))]

contract_grp <- data.table(sqlProc(server="prodbirpt", db="DWCORE",
                                   sqlFile="with accts as(
                                   SELECT 
                                   b.COMPANY_NUMBER AS infopro_div_nbr
                                   ,b.ACCOUNT_NUMBER as acct_nbr 
                                   ,b.SITE AS site_nbr
                                   ,ds.addr_line_1 
                                   ,ds.addr_line_2
                                   ,ds.city 
                                   ,ds.state 
                                   ,ds.postal_cd 
                                   ,ds.postal_cd_plus4 
                                   ,b.CONTAINER_GROUP AS container_grp_nbr
                                   ,case when b.charge_method = 'Q' then convert(money, b.CHARGE_RATE*dcg.Container_Cnt*dcg.Recurring_Charge_Freq) 
                                   else convert(money, b.CHARGE_RATE*dcg.Recurring_Charge_Freq) end as revenue_annual
                                   ,case when b.charge_method = 'Q' then convert(money, b.CHARGE_RATE*dcg.Container_Cnt) 
                                   else convert(money, b.CHARGE_RATE) end as revenue_per_bill
                                   
                                   , dcg.Container_size
                                   , convert(int, b.close_date)  as close_date
                                   , convert(int, b.EFFECTIVE_DATE) as EFFECTIVE_DATE
                                   , b.CHARGE_CODE
                                   , b.CHARGE_TYPE
                                   , b.CHARGE_METHOD
                                   ,count(*) over (partition by b.company_number, b.account_number, b.site, b.container_group, b.charge_rate) as dup_cc
                                   ,dcg.Revenue_Distribution_Cd
                                   ,dcg.Contract_Grp_Nbr
                                   ,dcg.Acct_Type
                                   ,dcg.Orig_Start_Dt
                                   ,dcg.Rate_Restrict_Dt
                                   ,dcg.Rate_Type,Stop_Cd
                                   ,dcg.District_Cd
                                   ,dcg.Recurring_Charge_Freq
                                   ,dcg.Seasonal_Stop_Dt
                                   ,dcg.Seasonal_Restart_Dt
                                   ,dcg.Close_Dt
                                   ,dcg.Disposal_Rate_Restrict_Dt
                                   ,dcg.is_Active
                                   ,dcg.Contract_Nbr
                                   ,dcg.Pickup_Period_Total_Lifts
                                   ,dcg.Pickup_Period_Length
                                   ,dcg.Pickup_Period_Unit,dcg.Container_Cnt,dcg.Container_Cd
                                   FROM dwpsa.dbo.STG_IFP_BIPCCR b
                                   join dwcore.dbo.Dim_Container_Grp dcg
                                   on b.COMPANY_NUMBER = dcg.Infopro_Div_Nbr 
                                   and b.ACCOUNT_NUMBER = dcg.Acct_Nbr
                                   and b.SITE = dcg.Site_Nbr
                                   and b.CONTAINER_GROUP = dcg.Container_Grp_Nbr
                                   and dcg.is_current =1
                                   join dwcore.dbo.dim_site ds
                                   on b.company_number = ds.infopro_div_nbr
                                   and b.account_number= ds.acct_nbr 
                                   and b.site= ds.site_nbr
                                   and ds.is_current=1
                                   WHERE b.CHARGE_TYPE = 'R' AND
                                   b.COMPANY_NUMBER in ('858','894','899','923','282', '320', '865')
                                   --and  (b.CLOSE_DATE<20170101 or b.CLOSE_DATE=0 ) 
                                   --and 	b.EFFECTIVE_DATE<=20160801
                                   and b.current_ind='Y'
                                   and dcg.Acct_Type NOT in ('X','T') 
                                   AND dcg.Contract_Grp_Nbr = '0'  )
                                   select
                                   infopro_div_nbr, acct_nbr, site_nbr, container_grp_nbr,
                                   EFFECTIVE_DATE, CLOSE_DATE, Container_Cnt, Pickup_Period_Total_Lifts,
                                   Pickup_Period_Length, Container_size, revenue_annual ,sum(revenue_per_bill) as revenue_per_bill 
                                   from accts
                                   group by 
                                   infopro_div_nbr, acct_nbr, site_nbr, container_grp_nbr,
                                   EFFECTIVE_DATE, CLOSE_DATE, Container_Cnt, Pickup_Period_Total_Lifts, 
                                   Pickup_Period_Length, Container_size, revenue_annual",
                                   asText=TRUE,windows_auth = TRUE, as.is = TRUE))

contract_grp[,`:=`(acct_nbr=paste0(sprintf("%03.f", as.numeric(infopro_div_nbr)), "-",
                                   sprintf("%07.f", as.numeric(acct_nbr)) ),
                   infopro_div_nbr=sprintf("%03.f", as.numeric(infopro_div_nbr)),
                   cont_grp=sprintf("%02.f", as.numeric(container_grp_nbr)))]

contract_grp[, ':='(future_rate = ifelse((effective_date>=20160801 & (close_date <= 20161231 & close_date != 0)), revenue_per_bill, '0'))]

mkt_acct <- copy(mkt)[, list(Account.Number)]
setkey(mkt_acct, Account.Number)
setkey(contract_grp, acct_nbr)
contract_grp <- contract_grp[mkt_acct]
# 
# Next_review_dt <- data.table(sqlProc(server="prodbirpt", db="DWPSA",
#                                      sqlFile="SELECT ifcomp, ifacct, CONVERT(DATETIME, CONVERT(CHAR(8), ifdat1)) 
#                                      AS next_review_dt FROM (SELECT ifcomp, ifacct,
#                                      case when ifdat1 > 30000000 or ifdat1 < 20000000 
#                                      or right(ifdat1,2) * 1 < 1 or right(ifdat1,2) * 1 > 31
#                                      or left(right(ifdat1,4),2) * 1 < 1
#                                      or left(right(ifdat1,4),2) * 1 > 12
#                                      then NULL else ifdat1 end as ifdat1, 
#                                      Row_number() 
#                                      OVER ( 
#                                      partition BY ac.ifcomp, ac.ifacct 
#                                      ORDER BY ac.dm_timestamp DESC) AS rn 
#                                      FROM stg_ifp_bipif AS ac 
#                                      WHERE ac.IFCOMP in ('858','894','899','923', '282', '320', '865')
#                                      AND ETL_BATCH_ID is not null ) AS z 
#                                      WHERE  rn = 1 and CONVERT(DATETIME, CONVERT(CHAR(8), ifdat1)) IS NOT NULL",
#                                      asText=TRUE,windows_auth = TRUE, as.is = TRUE))
# 
# 
# Next_review_dt[,`:=`(ifacct=sprintf("%07.f", as.numeric(ifacct)),
#                      ifcomp=sprintf("%03.f", as.numeric(ifcomp)))]
# 
# 
# next_review_dt_sub <- unique(Next_review_dt[,list(infopro_div_nbr=as.integer(ifcomp),
#                                                   account_number=paste0(ifcomp,"-", ifacct ),
#                                                   NRD=as.numeric(paste0(substr(next_review_dt,1,4),
#                                                                         substr(next_review_dt,6,7),
#                                                                         substr(next_review_dt,9,10))))])

# setkey(mkt_acct, Account.Number)
# setkey(next_review_dt_sub, account_number)
# next_review_dt_sub <- next_review_dt_sub[mkt_acct]

# setkey(mkt, InfoPro.Division.Number, Account.Number)
# setkey(next_review_dt_sub, infopro_div_nbr, account_number)
# mkt <- mkt[next_review_dt_sub]

################Email retention code#######

resi_cust_info <- data.table(sqlProc(server="prodbirpt", db="DWCORE",
                                     sqlFile="SELECT * FROM RPT_CUST_MKTG_INFO Where Customer_Category = 'Residential' 
                                     and Email != '' and Franchise = 'N' and National_Acct = 'N' and Acct_Type != 'Temp'",
                                     asText=TRUE,windows_auth = TRUE, as.is = TRUE))



resi_cust_info[,email:=gsub("\\.com.*",".com",email)]

resi_cust_info[,email:=gsub(" ","",email)]
resi_cust_info[,email:=gsub(",","",email)]
resi_cust_info[,email:=gsub("'","",email)]
resi_cust_info[,email:=gsub("\\/","",email)]

invalid_rec <- resi_cust_info[grepl("\\<[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}\\>", email, ignore.case=TRUE)==FALSE |
                                grepl("^none@",tolower(gsub(" ","",email))) == TRUE | 
                                grepl("@none.com",tolower(gsub(" ","",email))) == TRUE |
                                grepl("noemail",tolower(gsub(" ","",email))) == TRUE | 
                                grepl("@republicservices.com",tolower(gsub(" ","",email))) == TRUE]


resi_cust_info <- resi_cust_info[grepl("\\<[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}\\>", email, ignore.case=TRUE)==TRUE & 
                                   grepl("^none@",tolower(gsub(" ","",email))) == FALSE & 
                                   grepl("@none.com",tolower(gsub(" ","",email))) == FALSE & 
                                   grepl("noemail",tolower(gsub(" ","",email))) == FALSE &
                                   grepl("@republicservices.com",tolower(gsub(" ","",email))) == FALSE]


#For Minnesota

resi_cust_info <- resi_cust_info[infopro_div_nbr %in% c("858","894","899","923", "282", "320", "865")]


resi_cust_info[,c("postal1","postal2"):=tstrsplit(postal_code,split = "-",type.convert = TRUE)]



setkey(resi_cust_info,area,business_unit,lawson_company,
       infopro_div_nbr,account_number,container_grp)

resi_cust_info_sub <- unique(copy(resi_cust_info)[,list(area,business_unit,lawson_company,
                                                        infopro_div_nbr,account_number,
                                                        site_nbr=substr(container_grp,11,15),
                                                        cont_grp=substr(container_grp,16,17))])


#resi_cust_info <- dba_custserviceno[resi_cust_info]



setkey(resi_cust_info,area,business_unit,lawson_company,
       infopro_div_nbr,account_number)

resi_cust_info_sub_email <- unique(copy(resi_cust_info)[,list(area,business_unit,lawson_company,
                                                              infopro_div_nbr,account_number,
                                                              email,account_open_date 
                                                              , customer_name
                                                              , acct_address_line_1
                                                              , acct_address_line_2
                                                              , city
                                                              , state
                                                              , postal_code
                                                              , telephone_number_1)])
                                                              

################End################

resi_cust_info_sub_email[, `:=`(account_number = paste0(substr(account_number, 1,3), "-", substr(account_number,4,10)))]
resi_cust_tenure <- copy(resi_cust_info_sub_email)[, list(account_number, account_open_date)]

setkey(resi_cust_tenure, account_number)
setkey(mkt_acct, Account.Number)
resi_cust_tenure <- resi_cust_tenure[mkt_acct]

setkey(mkt, Account.Number)
setkey(resi_cust_tenure, account_number)
mkt <- mkt[resi_cust_tenure]

rm(resi_cust_tenure)

mkt[,`:=` (tenure_months = (as.yearmon(as.Date("2017-01-12"))-
                              as.yearmon(as.Date(account_open_date)))*12)]

sumByCols <- c("acct_nbr")
contract_grp_1 <- contract_grp[, lapply(.SD,sum),by=sumByCols,.SDcols=c("container_cnt")]

setkey(mkt_acct, Account.Number)
setkey(contract_grp_1, acct_nbr)
contract_grp_1 <- contract_grp_1[mkt_acct]

setkey(mkt, Account.Number)
setkey(contract_grp_1, acct_nbr)
mkt <- mkt[contract_grp_1]

contract_grp[, `:=`(Total_yard_month = ((1/as.numeric(pickup_period_length))*(52/12)*
                                          as.numeric(pickup_period_total_lifts)*as.numeric(container_size)))]


contract_grp_u <- copy(contract_grp)[, list(infopro_div_nbr, acct_nbr, site_nbr, container_grp_nbr, 
                                            container_size, revenue_annual, revenue_per_bill, Total_yard_month, future_rate)]

contract_grp_u[, `:=`(container_size = as.numeric(container_size),
                      revenue_annual = as.numeric(revenue_annual),
                      revenue_per_bill = as.numeric(revenue_per_bill), 
                      future_rate = as.numeric(future_rate))]

sumByCols <- c("infopro_div_nbr", "acct_nbr", "site_nbr", "container_grp_nbr")
contract_grp_u <- contract_grp_u[, lapply(.SD,sum),by=sumByCols,
                   .SDcols=c("container_size", "revenue_annual", "revenue_per_bill", "Total_yard_month", "future_rate")]


rate_upload[,`:=`(cont_grp = as.numeric(cont_grp))]
rate_upload[,`:=`(cont_grp = as.character(cont_grp))]

setkey(contract_grp_u, infopro_div_nbr, acct_nbr, site_nbr, container_grp_nbr)
setkey(rate_upload, infopro_div_nbr, acct_nbr, site_nbr, cont_grp)

# rate_upload_1 <- rate_upload[contract_grp_u]
contract_grp_u_1 <- contract_grp_u[rate_upload]
contract_grp_u_1 <- contract_grp_u_1[!is.na(revenue_annual)]
contract_grp_u_1 <- contract_grp_u_1[, `:=`(container_grp_nbr =NULL , i.revenue_annual =NULL, i.revenue_per_bill=NULL, site_nbr =NULL, 
                                            container_cd = NULL, close_date =NULL, infopro_div_nbr=NULL)]
contract_grp_u_1 <- unique(contract_grp_u_1)


sumByCols <- c( "acct_nbr", "RCF")
contract_grp_u_1 <- contract_grp_u_1[, lapply(.SD,sum),by=sumByCols,
                                     .SDcols=c("lifts_per_week", "quantity", "revenue_per_bill", "revenue_annual", "container_size", "Total_yard_month", "future_rate")]


setkey(mkt, Account.Number)
setkey(contract_grp_u_1, acct_nbr)
mkt_1 <- mkt[contract_grp_u_1]


takers_mn <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 1 - July 2016_MN.csv", header=TRUE))
takers_nj <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 1 - July 2016_NJ.csv", header=TRUE))
takers_littlerock <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 1 - July 2016_Little_Rock.csv", header=TRUE))
takers_1<- rbind(takers_mn, takers_nj, takers_littlerock)
takers_1[,`:=`(Wave_1= 1,
               Email.Address = NULL)]

declined <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 1 - July 2016_Declined.csv", header=TRUE))
declined[,`:=`(Declined = 1)]
declined <- declined[, list(Account.Number, Declined)]

takers_mn2 <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 2 - July 2016_MN.csv", header=TRUE))
takers_nj2 <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 2 - July 2016_NJ.csv", header=TRUE))
takers_littlerock2 <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 2 - July 2016_Little_Rock.csv", header=TRUE))
takers_2<- rbind(takers_mn2, takers_nj2, takers_littlerock2)
takers_2[,`:=`(Wave_2= 1, Email.Address = NULL)]

declined2 <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 2 - July 2016_Declined.csv", header=TRUE))
declined2[,`:=`(Declined = 1)]
declined2 <- declined2[, list(Account.Number, Declined)]


takers_mn3 <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 3 - July 2016_MN.csv", header=TRUE))
takers_nj3 <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 3 - July 2016_NJ.csv", header=TRUE))
takers_littlerock3 <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 3 - July 2016_Little_Rock.csv", header=TRUE))
takers_3<- rbind(takers_mn3, takers_nj3, takers_littlerock3)
takers_3[,`:=`(Wave_3= 1, Email.Address = NULL)]

declined3 <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 3 - July 2016_Declined.csv", header=TRUE))
declined3[,`:=`(Declined = 1)]
declined3 <- declined3[, list(Account.Number, Declined)]

declined <- rbind(declined, declined2, declined3)




# setkey(mkt_1, Account.Number)
# setkey(takers_1, Account.Number)
# mkt_2 <- merge(mkt_1, takers_1, by.x = "Account.Number", by.y = "Account.Number", all.x = TRUE)

setkey(mkt_1,Account.Number)
setkey(takers_1,Account.Number)

mkt_2 <- takers_1[mkt_1]

setkey(mkt_2,Account.Number)
setkey(takers_2,Account.Number)

mkt_3 <- takers_2[mkt_2]

setkey(mkt_3,Account.Number)
setkey(takers_3,Account.Number)
Mkt_4 <- takers_3[mkt_3]

setkey(mkt_acct, Account.Number)
setkey(declined, Account.Number)

declined <- declined[mkt_acct]
declined <- declined[, `:=`(Declined = ifelse(is.na(Declined), 0, 1))]

setkey(Mkt_4, Account.Number)
setkey(declined, Account.Number)

Mkt_4 <- Mkt_4[declined]

Mkt_4 <- Mkt_4[, `:=`(Date.Signed = as.character(Date.Signed),
                      i.Date.Signed = as.character(i.Date.Signed))]
Mkt_4[, `:=`(Date.Signed = coalesce(Date.Signed,i.Date.Signed))]
Mkt_4[, `:=`(i.Date.Signed = NULL)]

Mkt_4 <- Mkt_4[, `:=`(i.Date.Signed.1 = as.character(i.Date.Signed.1))]
Mkt_4[, `:=`(Date.Signed = coalesce(Date.Signed,i.Date.Signed.1))]
Mkt_4[, `:=`(i.Date.Signed.1 = NULL)]

Mkt_4 <- Mkt_4[, `:=`(Wave_1_Dt = as.Date("07/21/2016", "%m/%d/%Y"))]
Mkt_4 <- Mkt_4[, `:=`(Wave_2_Dt = as.Date("08/31/2016", "%m/%d/%Y"))]
Mkt_4 <- Mkt_4[, `:=`(Wave_3_Dt = as.Date("10/05/2016", "%m/%d/%Y"))]


exclusion_1 <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Exclusion_for_Wave2(Campaign1).csv", header=TRUE))
exclusion_1 <- exclusion_1[, `:=`(Email=NULL, exclusion_1=1)]

exclusion_2 <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Exclusion_for_Wave3(Campaign1).csv", header=TRUE))
exclusion_2 <- exclusion_2[, `:=`(Email=NULL, exclusion_2=1)]

setkey(mkt_acct, Account.Number)
setkey(exclusion_1, Account.Number)
exclusion_1 <- exclusion_1[mkt_acct]
exclusion_1 <- exclusion_1[, `:=`(exclusion_1 = ifelse(is.na(exclusion_1), 0, 1))]


setkey(mkt_acct, Account.Number)
setkey(exclusion_2, Account.Number)
exclusion_2 <- exclusion_2[mkt_acct]
exclusion_2 <- exclusion_2[, `:=`(exclusion_2 = ifelse(is.na(exclusion_2), 0, 1))]

setkey(Mkt_4, Account.Number)
setkey(exclusion_1, Account.Number)
Mkt_4 <- Mkt_4[exclusion_1]

setkey(Mkt_4, Account.Number)
setkey(exclusion_2, Account.Number)
Mkt_4 <- Mkt_4[exclusion_2]

Mkt_5 <- Mkt_4[,`:=`(Wave_2_Dt = ifelse(exclusion_1 == 1, as.Date(0, format = "%m/%d/%Y"), as.Date(Wave_2_Dt, format= "%m/%d/%Y")))]
Mkt_5 <- Mkt_5[,`:=`(Wave_2_Dt = as.Date(Wave_2_Dt))]
Mkt_5 <- Mkt_5[,`:=`(Wave_3_Dt = ifelse((exclusion_1 == 1 | exclusion_2 == 1), as.Date(0, format = "%m/%d/%Y"), as.Date(Wave_3_Dt, format = "%m/%d/%Y")))]
Mkt_5 <- Mkt_5[,`:=`(Wave_3_Dt = as.Date(Wave_3_Dt))]

# mkt_3[, `:=`(Wave_3='')]
contract_upload_1 <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Contract_Upload.csv", header=TRUE))
contract_upload_2 <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Contract_Upload_2.csv", header=TRUE))
contract_upload_3 <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Contract_Upload_NJ_AR_MN_wave_3.csv", header=TRUE))
contract_upload <- rbind(contract_upload_1, contract_upload_2, contract_upload_3)
contract_upload[,`:=`(site_nbr = NULL, Term = NULL, Date.Signed = NULL, Expiration_Date = NULL)]
contract_upload[,`:=`(acct_nbr=paste0(sprintf("%03.f", as.numeric(infopro_div_nbr)), "-",
                                      sprintf("%07.f", as.numeric(acct_nbr)) ))]
contract_upload[,`:=`(infopro_div_nbr=NULL)]

setkey(Mkt_5,Account.Number)
setkey(contract_upload, acct_nbr)
Mkt_5 <- contract_upload[Mkt_5]

# future_rate <- contract_grp <- data.table(sqlProc(server="prodbirpt", db="DWCORE",
#                                                   sqlFile="with accts as(
#                                                   SELECT 
#                                                   b.COMPANY_NUMBER AS infopro_div_nbr
#                                                   ,b.ACCOUNT_NUMBER as acct_nbr 
#                                                   ,b.SITE AS site_nbr
#                                                   ,ds.addr_line_1 
#                                                   ,ds.addr_line_2
#                                                   ,ds.city 
#                                                   ,ds.state 
#                                                   ,ds.postal_cd 
#                                                   ,ds.postal_cd_plus4 
#                                                   ,b.CONTAINER_GROUP AS container_grp_nbr
#                                                   ,case when b.charge_method = 'Q' then b.CHARGE_RATE*dcg.Container_Cnt*dcg.Recurring_Charge_Freq 
#                                                   else b.CHARGE_RATE*dcg.Recurring_Charge_Freq end as revenue_ann
#                                                   ,case when b.charge_method = 'Q' then b.CHARGE_RATE*dcg.Container_Cnt 
#                                                   else b.CHARGE_RATE end as revenue_per_bill
#                                                   
#                                                   , dcg.Container_size
#                                                   , b.close_date 
#                                                   , b.EFFECTIVE_DATE
#                                                   , b.CHARGE_CODE
#                                                   , b.CHARGE_TYPE
#                                                   , b.CHARGE_METHOD
#                                                   ,count(*) over (partition by b.company_number, b.account_number, b.site, b.container_group, b.charge_rate) as dup_cc
#                                                   ,dcg.Revenue_Distribution_Cd
#                                                   ,dcg.Contract_Grp_Nbr
#                                                   ,dcg.Acct_Type
#                                                   ,dcg.Orig_Start_Dt
#                                                   ,dcg.Rate_Restrict_Dt
#                                                   ,dcg.Rate_Type,Stop_Cd
#                                                   ,dcg.District_Cd
#                                                   ,dcg.Recurring_Charge_Freq
#                                                   ,dcg.Seasonal_Stop_Dt
#                                                   ,dcg.Seasonal_Restart_Dt
#                                                   ,dcg.Close_Dt
#                                                   ,dcg.Disposal_Rate_Restrict_Dt
#                                                   ,dcg.is_Active
#                                                   ,dcg.Contract_Nbr
#                                                   ,dcg.Pickup_Period_Total_Lifts
#                                                   ,dcg.Pickup_Period_Length
#                                                   ,dcg.Pickup_Period_Unit,dcg.Container_Cnt,dcg.Container_Cd
#                                                   FROM dwpsa.dbo.STG_IFP_BIPCCR b
#                                                   join dwcore.dbo.Dim_Container_Grp dcg
#                                                   on b.COMPANY_NUMBER = dcg.Infopro_Div_Nbr 
#                                                   and b.ACCOUNT_NUMBER = dcg.Acct_Nbr
#                                                   and b.SITE = dcg.Site_Nbr
#                                                   and b.CONTAINER_GROUP = dcg.Container_Grp_Nbr
#                                                   and dcg.is_current =1
#                                                   join dwcore.dbo.dim_site ds
#                                                   on b.company_number = ds.infopro_div_nbr
#                                                   and b.account_number= ds.acct_nbr 
#                                                   and b.site= ds.site_nbr
#                                                   and ds.is_current=1
#                                                   WHERE b.CHARGE_TYPE = 'R' AND
#                                                   b.COMPANY_NUMBER in ('858','894','899','923','282', '320', '865')
#                                                   and  (b.CLOSE_DATE<20170101 ) 
#                                                   and 	b.EFFECTIVE_DATE>=20160801
#                                                   and b.current_ind='Y'
#                                                   and dcg.Acct_Type NOT in ('X','T') 
#                                                   AND dcg.Contract_Grp_Nbr = '0'  )
#                                                   select
#                                                   infopro_div_nbr, acct_nbr, site_nbr, container_grp_nbr,
#                                                   EFFECTIVE_DATE, CLOSE_DATE
#                                                   ,sum(revenue_per_bill) as future_revenue_per_bill
#                                                   
#                                                   from accts 
#                                                   
#                                                   group by 
#                                                   infopro_div_nbr, acct_nbr, site_nbr, container_grp_nbr,
#                                                   EFFECTIVE_DATE, CLOSE_DATE",
#                                                   asText=TRUE,windows_auth = TRUE, as.is = TRUE))
# 
# future_rate <- future_rate[,`:=`(account_number=paste0(sprintf("%03.f", as.numeric(infopro_div_nbr)), "-",
#                                                        sprintf("%07.f", as.numeric(acct_nbr)) ))]
# 
# setkey(future_rate, account_number)
# setkey(mkt_acct, Account.Number)
# future_rate <- future_rate[mkt_acct]
# future_rate <- future_rate[!is.na(acct_nbr)]
# future_rate <- future_rate[close_date != 0]
# 
# future_rate <- future_rate[,`:=`(infopro_div_nbr = NULL, acct_nbr= NULL, site_nbr= NULL, container_grp_nbr = NULL, 
#                                  effective_date = NULL, close_date = NULL, 
#                                  future_revenue_per_bill = as.numeric(future_revenue_per_bill))]
# 
# 
# future_rate <- future_rate[, lapply(.SD,sum),by="account_number",
#                            .SDcols=c("future_revenue_per_bill")]
# 
# setkey(future_rate, account_number)
# setkey(mkt_acct, Account.Number)
# future_rate_1 <- future_rate[mkt_acct]
# 
# setkey(future_rate_1, account_number)
# setkey(mkt_3, acct_nbr)
# mkt_4 <- mkt_3[future_rate_1]

Mkt_5 <- Mkt_5[,`:=`(Profitability='', Cat_Rank='', exclusion_2=NULL, exclusion_1=NULL)]

Mkt_5 <- Mkt_5[,`:=`(Account_Number = as.numeric(substr(acct_nbr, 5,11)))]

# mkt_5 <- copy(mkt_4)[, list(InfoPro.Division.Number, acct_nbr, Account_Number, Customer.Service.Number, Email, Service.Zip, 
#                             revenue_per_bill, revenue_annual, future_revenue_per_bill, Year_1, Year_2, Year_3, 
#                             tenure_months, RCF, Profitability, Cat_Rank, INV, EVR, FRF, Date.Signed, 
#                             container_cnt, Total_yard_month, RCY, SW, account_open_date, NRD, contract_number,
#                             Group,  Wave_1, Wave_2, Wave_3)]
# 
# NAs <- copy(mkt_5)[is.na(Email)]
# mkt_5 <- mkt_5[!is.na(Email)]
# mkt_5 <- unique(mkt_5)

setcolorder(Mkt_5, c("InfoPro.Division.Number", "Account_Number", "Wave_1_Dt", "Wave_2_Dt", "Wave_3_Dt", "Wave_1", "Wave_2", 
            "Wave_3", "Declined", "Date.Signed", "contract_number", "Customer.Service.Number", "Doing.Business.As", "Email", 
            "Customer.Name", "Billing.Address", "Billing.City", "Billing.State", "Billing.Zip", "Telephone", "RCY", "SW", "Service.Address", 
            "Service.City", "Service.State", "Service.Zip",  "INV", "EVR", "FRF", "Group", "Year_1", "Year_2", "Year_3", 
            "account_open_date", "tenure_months", "container_cnt", "RCF", "lifts_per_week", "quantity", "revenue_per_bill",
            "revenue_annual", "future_rate", "container_size", "Total_yard_month", "acct_nbr", "Profitability", "Cat_Rank"))

Mkt_6 <- unique(Mkt_5, by = "acct_nbr")

Mkt_6 <- Mkt_6[, `:=`(Year_1 = round(Year_1, 2), 
                      Total_yard_month = round(Total_yard_month, 2),
                      tenure_months = round(tenure_months, 2))]

Mkt_7 <- Mkt_6[!(InfoPro.Division.Number %in% c('282', '320'))]

sqlInsert("prodpricsql01", "Marketing_Campaign", "Mkt_Analysis_1", Mkt_7, windows_auth = TRUE)

save.image("C:/Users/gadaji/Desktop/EMail_Retention_Database.Rdata")

# conect <- odbcConnect("prodpricsql01")
# 
# sqlSave(conect, Mkt_6, tablename = "Mkt_Analysis_1", rownames = FALSE, colnames = TRUE)



mkt <- mkt[,`:=`(Account_Number = as.numeric(substr(Account.Number, 5,11)))]

sqlSave(conect, mkt, tablename = "Mkt_Send_Wave1_MnNJAr", rownames = FALSE, colnames = FALSE)


nrow(mkt_5)
rm(mkt_5)
