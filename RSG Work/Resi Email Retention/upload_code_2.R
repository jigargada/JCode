
library(data.table)
library(RODBC)
library(xlsx)
library(stringr)
library(rsgutilr)

load("//prodpricapp02/d/Analytics/Resi Email Retention/rate_upload.robj")
mkt <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Email Retention Cust_Data (for Marketing dept)_V4.csv", header = TRUE))
# success = data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Upload Success.csv", header=TRUE))
# success_1 = data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Test Upload Success.csv", header=TRUE))

contract_grp <- data.table(sqlProc(server="prodbirpt", db="DWCORE",
                                   sqlFile="with accts as (SELECT 
                                   b.COMPANY_NUMBER AS infopro_div_nbr
                                   ,b.ACCOUNT_NUMBER as acct_nbr 
                                   ,b.SITE AS site_nbr
                                   ,ds.addr_line_1 
                                   ,ds.addr_line_2
                                   ,ds.city 
                                   ,ds.state 
                                   ,ds.postal_cd 
                                   ,ds.postal_cd_plus4
                                   ,ds.contract_nbr as Site_Contract_Nbr
                                   ,b.CONTAINER_GROUP AS container_grp_nbr
                                   ,case when b.charge_method = 'Q' then b.CHARGE_RATE*dcg.Container_Cnt*dcg.Recurring_Charge_Freq 
                                   else b.CHARGE_RATE*dcg.Recurring_Charge_Freq end as revenue_ann
                                   ,case when b.charge_method = 'Q' then b.CHARGE_RATE*dcg.Container_Cnt 
                                   else b.CHARGE_RATE end as revenue_per_bill
                                   
                                   , dcg.Container_size
                                   , b.close_date 
                                   , b.EFFECTIVE_DATE
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
                                   b.COMPANY_NUMBER in ('858','894','899','923', '865')
                                   and dcg.is_Active = 1
                                   and dcg.is_Current = 1
                                   --and dcg.Close_Dt = '9999-12-31 00:00:00.000'
                                   and  (b.CLOSE_DATE>20160701 or b.close_date = 0) 
                                   and 	b.EFFECTIVE_DATE<=20160701
                                   and b.current_ind='Y'
                                   and dcg.Acct_Type NOT in ('X','T') 
                                   AND dcg.Contract_Grp_Nbr = '0'  
                                   and  ds.is_Current = 1
                                   and b.Billing_adj_flg = '0'
                                   and ds.Close_Dt > '2017-06-30'
                                   and b.CHARGE_RATE > 0.00)
                                   
                                   select
                                   addr_line_1,addr_line_2 ,city ,state ,postal_cd,postal_cd_plus4
                                   ,infopro_div_nbr, acct_nbr, site_nbr, container_grp_nbr
                                   ,Revenue_Distribution_Cd, Contract_Grp_Nbr, Acct_Type, Site_Contract_Nbr, Orig_Start_Dt
                                   ,Rate_Restrict_Dt, Rate_Type,Stop_Cd
                                   ,District_Cd,Recurring_Charge_Freq
                                   ,Seasonal_Stop_Dt,Seasonal_Restart_Dt
                                   ,Close_Dt,Disposal_Rate_Restrict_Dt
                                   ,is_Active,Contract_Nbr
                                   ,Pickup_Period_Total_Lifts
                                   ,Pickup_Period_Length
                                   ,Pickup_Period_Unit,Container_Cnt,Container_Cd
                                   , Container_size
                                   , close_date 
                                   , EFFECTIVE_DATE
                                   , CHARGE_CODE
                                   , CHARGE_TYPE
                                   , CHARGE_METHOD
                                   ,max(dup_cc) as dup_cc
                                   ,sum(revenue_ann) as revenue_annual
                                   ,sum(revenue_per_bill) as revenue_per_bill
                                   
                                   from accts 
                                   group by 
                                   infopro_div_nbr, acct_nbr, site_nbr, container_grp_nbr
                                   ,Revenue_Distribution_Cd, Contract_Grp_Nbr, Acct_Type, Site_Contract_Nbr, Orig_Start_Dt
                                   ,Rate_Restrict_Dt, Rate_Type,Stop_Cd
                                   ,District_Cd,Recurring_Charge_Freq
                                   ,Seasonal_Stop_Dt,Seasonal_Restart_Dt
                                   ,Close_Dt,Disposal_Rate_Restrict_Dt
                                   ,is_Active,Contract_Nbr
                                   ,Pickup_Period_Total_Lifts
                                   ,Pickup_Period_Length
                                   ,Pickup_Period_Unit,Container_Cnt,Container_Cd, Container_size
                                   , close_date 
                                   , EFFECTIVE_DATE
                                   , CHARGE_CODE
                                   , CHARGE_TYPE
                                   , CHARGE_METHOD
                                   ,addr_line_1,addr_line_2 ,city ,state ,postal_cd,postal_cd_plus4
                                   ",
                                   asText=TRUE,windows_auth = TRUE, as.is = TRUE))

contract_grp[,`:=`(acct_nbr=paste0(sprintf("%03.f", as.numeric(infopro_div_nbr)), "-",
                                   sprintf("%07.f", as.numeric(acct_nbr)) ),
                   infopro_div_nbr=sprintf("%03.f", as.numeric(infopro_div_nbr)),
                   site_nbr=as.numeric(site_nbr),
                   cont_grp=sprintf("%02.f", as.numeric(container_grp_nbr)))]



takers_mn <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 2 - July 2016_MN.csv", header=TRUE))
takers_nj <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 2 - July 2016_NJ.csv", header=TRUE))
takers_littlerock <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 2 - July 2016_Little_Rock.csv", header=TRUE))

takers<- rbind(takers_mn, takers_nj, takers_littlerock)

setkey(mkt, Account.Number)
setkey(takers, Account.Number)
takers <- mkt[takers]

rate_upload[, `:=`(acct_nbr = paste0(substr(acct_nbr, 1,3), "-", substr(acct_nbr, 4,10)))]
rate_upload[, `:=`(site_nbr = as.numeric(site_nbr),
                   cont_grp = as.numeric(cont_grp))]

takers_1 <- copy(takers)[, list(Account.Number, Year_1, Year_2, Year_3, Date.Signed, Group)]
#rate_upload[,`:=`(acct_nbr = paste0(infopro_div_nbr, sprintf("%07.f", as.numeric(acct_nbr))))]
# success <- success[,`:=`(Account = paste0(Company, "-", sprintf("%07.f", as.numeric(Account))))]
# success <- success_1[,`:=`(Account.number = paste0(Company.Number, "-", sprintf("%07.f", as.numeric(Account.number))))]

setkey(takers_1, Account.Number)
setkey(rate_upload, acct_nbr)
rate_upload_1 <- rate_upload[takers_1]


# setkey(rate_upload_1, acct_nbr, site_nbr, cont_grp)
# setkey(success, Account, Site, Container.Group)
# 
# rate_upload_2 <- rate_upload_1[!success]

contract_grp_1  <- contract_grp[, list(acct_nbr, site_nbr, container_grp_nbr, revenue_distribution_cd, container_cd, charge_code,
                                       charge_type, charge_method, recurring_charge_freq, revenue_per_bill, effective_date)]
rate_upload_1  <- rate_upload_1[,`:=`(revenue_per_bill= as.character(revenue_per_bill),
                                      cont_grp = as.character(cont_grp))]

# contract_grp[, `:=`(addr_line_1=NULL, addr_line_2=NULL, city=NULL, state=NULL, postal_cd=NULL, postal_cd_plus4=NULL, 
#                     infopro_div_nbr=NULL, contract_grp_nbr=NULL, acct_type=NULL, site_contract_nbr=NULL, orig_start_dt=NULL, 
#                     rate_restrict_dt=NULL, rate_type, stop_cd=NULL, district_cd=NULL, recurring_charge_freq=NULL, seasonal_stop_dt=NULL,
#                     seasonal_restart_dt=NULL, close_dt=NULL, disposal_rate_restrict_dt=NULL, is_active=NULL, contract_nbr=NULL, 
#                     pickup_period_total_lifts=NULL, pickup_period_length=NULL, pickup_period_unit=NULL, container_cnt=NULL, container_size=NULL,
#                     close_date=NULL, effective_date=NULL, charge_type=NULL, charge_method=NULL, dup_cc=NULL, revenue_annual=NULL, 
#                     revenue_per_bill=NULL, cont_grp=NULL)]


setkey(contract_grp_1, acct_nbr, site_nbr, container_grp_nbr)
setkey(rate_upload_1, acct_nbr, site_nbr, cont_grp)
rate_upload_3 <- contract_grp_1[rate_upload_1]

rate_upload_3[, `:=`(i.revenue_per_bill =NULL, i.container_cd=NULL)]

#rate_upload_3[, `:=`(Year_1itemized = as.numeric(revenue_per_bill)/(12/as.numeric(RCF)))]
invalid <- rate_upload_3[is.na(revenue_per_bill)]

rate_upload_4 <- copy(rate_upload_3)[, ':='(effective_date = as.Date(paste0(substr(effective_date,1,4), "-" ,substr(effective_date,5,6),
                                                     "-" ,substr(effective_date,7,8))),
                     Date.Signed = as.Date(Date.Signed, format = "%m/%d/%Y"))]

rate_upload_4[, ':='(eff_date = as.Date(ifelse(effective_date>Date.Signed, effective_date, Date.Signed), origin = "1970-01-01")+365)]
rate_upload_4[, ':='(Year_1itemized = ifelse(Group == "A11" | Group == "A12", round(as.numeric(revenue_per_bill) + 
                   as.numeric(revenue_per_bill)*0.04, digits = 2), ifelse(Group == "A21" | Group == "A22", 
                    round(as.numeric(revenue_per_bill) + as.numeric(revenue_per_bill)*0.1, digits = 2), NA_character_)))] 

contract_uploads = data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Contract_Upload_2.csv", header=TRUE))
contract_uploads[, `:=`(acct_nbr=paste0(sprintf("%03.f", as.numeric(infopro_div_nbr)), "-" ,
                       sprintf("%07.f", as.numeric(acct_nbr))))]
contract_uploads_1 <- contract_uploads[,list(acct_nbr,contract_number, site_nbr)]

setkey(rate_upload_4, acct_nbr, site_nbr)
setkey(contract_uploads_1, acct_nbr, site_nbr)
rate_upload_4 <- rate_upload_4[contract_uploads_1]

rate_upload_4[, `:=`(acct_nbr = as.numeric(substr(acct_nbr, 5,11)),
                     site_nbr = sprintf("%05.f", site_nbr))]

tax_app_code <- data.table(sqlProc(server="prodbirpt", db="DWPSA",
                                   sqlFile="SELECT [COMPANY_NUMBER]
		                                ,convert(int, [ACCOUNT_NUMBER]) as [ACCOUNT_NUMBER]
                                   ,[SITE] 
                                   ,[CONTAINER_GROUP]
                                   ,[CHARGE_CODE]
                                   ,[CHARGE_RATE]
                                   ,[CHARGE_TYPE]
                                   ,[CHARGE_METHOD]
                                   ,[TAX_APP_CODE]
                                   FROM [DWPSA].[dbo].[STG_IFP_BIPCCR]
                                   where [COMPANY_NUMBER] in (858, 894, 899, 923, 865)
                                   and [CLOSE_DATE] = 0 "
                                   ,asText=TRUE,windows_auth = TRUE, as.is = TRUE))

rate_upload_4 <- rate_upload_4[!(is.na(revenue_distribution_cd)| is.na(recurring_charge_freq))]

setkey(tax_app_code,  company_number, account_number, site, container_group, charge_code, charge_rate, charge_type, charge_method)
setkey(rate_upload_4, infopro_div_nbr, acct_nbr, site_nbr, container_grp_nbr, charge_code, revenue_per_bill, charge_type, charge_method)
rate_upload_4 <- tax_app_code[rate_upload_4]

upload  <- rate_upload_4[, list(company_number, account_number, site, container_group, effective_date, 
                               eff_date, close_date, charge_code, charge_type, charge_method, tax_app_code, charge_rate,
                              Year_1itemized, contract_number)]

write.table(upload, paste0("//prodpricapp02/d/Analytics/Resi Email Retention/Rate_Upload_Takers2.csv"),
            sep=",",na="",row.names=FALSE)

rate_upload_5 <- copy(rate_upload_4)[, `:=`(close_date = NULL)]
rate_upload_5 <- rate_upload_5[, `:=`(effective_date = as.Date(eff_date))]
rate_upload_5 <- rate_upload_5[, `:=`(charge_rate = Year_1itemized)]

rate_upload_5[, `:=`(eff_date=effective_date+365)]
rate_upload_5[, ':='(Year_2itemized = ifelse(Group == "A11" | Group == "A12", round(as.numeric(Year_1itemized) + 
            as.numeric(Year_1itemized)*0.04, digits = 2), ifelse(Group == "A21" | Group == "A22", 
            round(as.numeric(Year_1itemized) + as.numeric(Year_1itemized)*0.1, digits = 2), NA_character_)))] 

upload_2  <- rate_upload_5[, list(company_number, account_number, site, container_group, effective_date, 
                                eff_date, charge_code, charge_type, charge_method, tax_app_code, charge_rate,
                                Year_2itemized, contract_number)]


write.table(upload_2, paste0("//prodpricapp02/d/Analytics/Resi Email Retention/Rate_Upload_Takers2_Y3.csv"),
           sep=",",na="",row.names=FALSE)

