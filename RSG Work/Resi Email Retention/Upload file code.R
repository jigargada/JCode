
library(data.table)
library(RODBC)
library(xlsx)
library(stringr)
library(rsgutilr)
load("//prodpricapp02/d/Analytics/Resi Email Retention/rate_upload.robj")
mkt <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Email Retention Cust_Data (for Marketing dept)_V4.csv", header = TRUE))
success = data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Upload Success.csv", header=TRUE))

takers_mn <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 1 - July 2016_MN.csv", header=TRUE))
takers_nj <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 1 - July 2016_NJ.csv", header=TRUE))
takers_littlerock <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 1 - July 2016_Little_Rock.csv", header=TRUE))

takers<- rbind(takers_mn, takers_nj, takers_littlerock)

setkey(mkt, Account.Number)
setkey(takers, Account.Number)
takers <- mkt[takers]

takers[,`:=`(flag = 1)]
takers <- takers[, `:=` (Date.Signed = paste0(Date.Signed, "-2016"))]
takers <- takers[,`:=` (Date.Signed= gsub("-","",(as.Date(Date.Signed, format="%d-%B-%Y"))))]

rate_upload[, `:=`(acct_nbr = paste0(substr(acct_nbr, 1,3), "-", substr(acct_nbr, 4,10)))]
setkey(rate_upload, acct_nbr)
setkey(takers, Account.Number)

rate_upload <- takers[rate_upload]
rate_upload <- rate_upload[!flag !='1' ]
rate_upload <- rate_upload[!Group =='HO']
# rate_upload <- rate_upload[, ':=' (New_Rate_per_bill = ifelse(Group =='A11', revenue_per_bill*1.04,
#                                                      ifelse(Group =='A12', revenue_per_bill*1.04, 
#                                                             ifelse(Group =='A21', revenue_per_bill*1.1,
#                                                                    ifelse(Group =='A22', revenue_per_bill*1.1, "")))))]
rate_upload <- rate_upload[, Acct:=as.integer(substr(Account.Number, 5,11))]
rate_upload <- rate_upload[, Site:= as.integer(site_nbr)]
rate_upload <- rate_upload[, CGrp:= as.integer(cont_grp)]
rate_upload <- rate_upload[, ':='(Customer.Service.Number = NULL, Doing.Business.As = NULL, 
                                  Email= NULL, Billing.Address = NULL, Billing.City = NULL,
                                  Customer.Name =NULL, Billing.State = NULL, Billing.Zip = NULL, Telephone = NULL,
                                  RCY = NULL, SW = NULL, Service.Address = NULL, Service.City = NULL, 
                                  Service.State = NULL, Service.Zip = NULL, INV = NULL, EVR = NULL, 
                                  FRF = NULL, flag = NULL, 
                                  infopro_div_nbr = NULL, quantity = NULL, container_cd = NULL,
                                  close_date = NULL, revenue_annual = NULL, Account.Number =NULL,
                                  site_nbr = NULL, cont_grp = NULL)]

rates_table <- data.table(sqlProc(server="prodbirpt", db="DWCORE",
                                  sqlFile="SELECT convert(int, COMPANY_NUMBER) as COMPANY_NUMBER
                                  ,convert(int, [ACCOUNT_NUMBER]) as [ACCOUNT_NUMBER]
                                  ,convert(int, [SITE]) as [SITE]
                                  ,convert(int, CONTAINER_GROUP) as CONTAINER_GROUP,  
                                  
                                  [CHARGE_CODE], [CHARGE_TYPE], [CHARGE_METHOD], convert(int, effective_date) as effective_date, [CLOSE_DATE],convert(varchar, CHARGE_RATE) as CHARGE_RATE,
                                  convert (varchar(8), (CONVERT(varchar(8), GETDATE(), 112))) as curr_date,
                                  case when convert(varchar(8), (CONVERT(varchar(8), GETDATE(), 112))) between effective_date and [CLOSE_DATE]
                                  then convert(int, CHARGE_RATE)
                                  when (CONVERT(varchar(8), GETDATE(), 112) > EFFECTIVE_DATE and CLOSE_DATE = 0) then 
                                  convert(int, CHARGE_RATE)
                                  Else ' '
                                  End as current_rate,
                                  Row_number() 
                                  OVER ( 
                                  partition BY company_number, account_number, site, 
                                  container_group, charge_code, charge_type,
                                  charge_method
                                  ORDER BY DM_TIMESTAMP DESC, effective_date desc) AS rn,
                                  case when effective_date > '20160820' then convert(int, effective_date)
                                  Else ' '
                                  End as Future_Date,
                                  case when effective_date > '20160820' then convert(int, CHARGE_RATE)
                                  Else ' '
                                  End as Future_Rate
                                  FROM [DWPSA].[dbo].[STG_IFP_BIPCCR]
                                  where COMPANY_NUMBER in ('858','894','899','923', '282', '320', '865')
                                  /*and (case when effective_date > '20160901' then convert(varchar, CHARGE_RATE)
                                  Else ' '
                                  End) != ''
                                  and  (case when convert(int, (CONVERT(varchar(8), GETDATE(), 112))) between effective_date and [CLOSE_DATE]
                                  then convert(varchar, CHARGE_RATE)
                                  Else ' '
                                  End) != 0 
                                  */
                                  and (CLOSE_DATE > (CONVERT(varchar(8), GETDATE(), 112))
                                  or (effective_date > '20160820')
                                  or CLOSE_DATE = 0)
                                  and CHARGE_TYPE != 'W'
                                  and CURRENT_IND = 'Y'
                                  and [SPLIT_IND] = 'N'
                                  and [MERGE_IND] = 'N'
                                  and [CHARGE_CODE] not in ('ncc', 'nch', 'rcc', 'frx', 'fre', 'rsc', 'don', 
                                  'yar', 'rc1', 'del', 'dsp', 'nrs')",
                                  asText=TRUE,windows_auth = TRUE, as.is = TRUE))

#rates_table <- rates_table[!rn !='1' ]


# rates_table_1 <- rates_table[, lapply(.SD[current_rate, future_rate], sum), by = c("company_number","account_number",
#                                                                                               "site", "container_group", "charge_code"
#                                                                                               ,"charge_type", "charge_method")]

setkey(rates_table, company_number, account_number, site, container_group)
setkey(rate_upload, InfoPro.Division.Number, Acct, Site, CGrp)

#str(rates_table)
#str(rate_upload)
table_full <- rates_table[rate_upload]

setkey(rates_table, company_number, account_number, site, container_group)
setkey(table_full, company_number, account_number, site, container_group)

table_full <- table_full[!success]

table_full <- table_full [, ':=' (Percent_increase= ifelse(substr(Group, 2,2) == '1', 0.04,
                                                           ifelse(substr(Group, 2,2) == '2', 0.1,'')      ))]

table_full <- table_full[,':='(Email.Address=NULL, lifts_per_week=NULL, Group = NULL, rn=NULL)]


cont_details <- data.table(sqlProc(server="prodbirpt", db="DWCORE",
                                  sqlFile="Select convert(int, [Infopro_Div_Nbr]) as InfoPro_Div_Nbr, 
                                  convert(int, [Acct_Nbr]) as Account_Number, 
                                  convert(int,[Site_Nbr]) as Site_Nbr, convert(int, [Container_Grp_Nbr]) as Container_Grp_Nbr, [Container_Cd], 
                                  [Container_Size], [Container_Cnt]
                                  FROM [DWCORE].[dbo].[Dim_Container_Grp]
                                  where [is_Active] = 1
                                  and [is_Current] = 1
                                  --and [Rate_Type] = 'R'
                                  --and [Rate_Restrict_Dt] ='9999-12-31 00:00:00.000'
                                  and Infopro_Div_Nbr in ('858','894','899','923', '282', '320', '865')
                                  and [is_Split] = 0
                                  and [is_Merged] = 0",
                                  asText=TRUE,windows_auth = TRUE, as.is = TRUE))

setkey(cont_details, infopro_div_nbr, account_number, site_nbr, container_grp_nbr)
setkey(table_full, company_number, account_number, site, container_group)

table_final <- cont_details[table_full]
# write.table(table_final, paste0("//prodpricapp02/d/Analytics/Resi Email Retention/Resi_Takers.csv"),
#             sep=",",na="",row.names=FALSE)


setkey(table_final, infopro_div_nbr, account_number, site_nbr, container_grp_nbr)
setkey(success, Company, Account, Site, Container.Group )
table_final_1 <- table_final[!success]



# 
# table_final_1 <- table_final_1[, `:=` (effective_date_1 = ifelse((effective_date < 
#                                           (gsub("-", "",(as.Date(Sys.Date(), 
#                                           format = "%Y%m%d")))) & (substr(close_date, 1,4)=='2016')) 
#                                           , (gsub("-", "",(as.Date(close_date, format = "%Y%m%d")+1)))  #condition for 1st if
#                                           , ifelse((effective_date > 
#                                                       (gsub("-", "",(as.Date(Sys.Date(), 
#                                                                              format = "%Y%m%d")))) (substr(effective_date, 1,4)=='2016') & (close_date == '0')) 
#                                                    , (gsub("-", "",(as.Date(effective_date, format = "%Y%m%d")+1)))  #condition for 1st if
#                                                    , ifelse((effective_date < 
#                                                       (gsub("-", "",(as.Date(Sys.Date(), format= "%Y%m%d"))))) & (close_date == '0'),
#                                                       (gsub("-", "",(as.Date(Date.Signed, format = "%Y%m%d")+365))) & (current_rate = charge_rate*+percent_Increase),
#                                                       ,''
#                                                       )
#                                                    
#                                                    )))]

# table_final_2 <- table_final_1[close_date == 0, `:=` (effective_date_1 = ifelse((as.character(effective_date) < (gsub("-", "",(as.Date(Sys.Date(), 
#                                format = "%Y%m%d"))))) & (substr(effective_date, 1,4)=='2016'), (gsub("-", "",(as.Date(Date.Signed, format = "%Y%m%d")+365))), 
#                                (gsub("-", "",(as.Date(as.character(effective_date), format = "%Y%m%d")+1)))))]


table_final_1[, `:=` ( charge_rate= NULL, curr_date= NULL, Year_1= NULL, Year_2= NULL, Year_3= NULL, RCF= NULL, Date.Signed = NULL
                       , revenue_per_bill = NULL, Percent_increase=NULL, close_date= NULL, effective_date= NULL )] 

table_final_2 <- table_final_1[,lapply(.SD, sum), by = c("infopro_div_nbr", "account_number", "site_nbr", 
                                                         "container_grp_nbr", "container_cd", "container_size", "container_cnt",
                                                         "charge_code", "charge_type", "charge_method"),
                               .SDcols = c("current_rate", "future_rate", "future_date")]  
table_final_2 <- table_final_2[!future_date == 0]

# table_final_1 <- table_final_1[, `:=` (effective_date_1 = ifelse((effective_date > 
#                                         (gsub("-", "",(as.Date(Sys.Date(), 
#                                          format = "%Y%m%d")))) & (substr(effective_date, 1,4)=='2016') & (close_date = 0)) 
#                                      , (gsub("-", "",(as.Date(close_date, format = "%Y%m%d")+1)))  #condition for 1st if
#                                      , ''))]
     
table_final_2[, ':=' (effective_date_1= as.numeric(table_final_2$effective_date_1))]
table_final_2[is.na(table_final_2 <- effective_date_1)] <- 0


table_final_2[, `:=` (effective_date = NULL, close_date= NULL, charge_rate= NULL, 
                      curr_date= NULL, future_date= NULL, Year_1= NULL, Year_2= NULL, Year_3= NULL,
                      Date.Signed= NULL, RCF= NULL, revenue_per_bill= NULL, Percent_increase= NULL
                      )] 



# table_final_2[which(is.na(current_rate)),] <- as.numeric('0')
# table_final_2[which(is.na(future_rate)),] <- as.numeric('0')


table_final_3 <- table_final_2[,lapply(.SD, sum), by = c("infopro_div_nbr", "account_number", "site_nbr", 
                                    "container_grp_nbr", "container_cd", "container_size", "container_cnt",
                                    "charge_code", "charge_type", "charge_method"),
                                    .SDcols = c("current_rate", "future_rate", "effective_date_1")]                                           

table_final_3 <- table_final_3[,lapply(.SD,sum),by=sumByCols,.SDcols=c("lifts_per_week","revenue_annual", "revenue_per_bill")]