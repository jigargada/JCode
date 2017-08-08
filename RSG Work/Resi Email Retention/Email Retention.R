library(data.table)
library(RODBC)
library(xlsx)
library(rsgutilr)

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

nrow(invalid_rec)
resi_cust_info <- resi_cust_info[grepl("\\<[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}\\>", email, ignore.case=TRUE)==TRUE & 
                                   grepl("^none@",tolower(gsub(" ","",email))) == FALSE & 
                                   grepl("@none.com",tolower(gsub(" ","",email))) == FALSE & 
                                   grepl("noemail",tolower(gsub(" ","",email))) == FALSE &
                                   grepl("@republicservices.com",tolower(gsub(" ","",email))) == FALSE]
nrow(resi_cust_info)

#For Minnesota

resi_cust_info <- resi_cust_info[infopro_div_nbr %in% c("858","894","899","923", "282", "320", "865")]

nrow(resi_cust_info)

resi_cust_info[,c("postal1","postal2"):=tstrsplit(postal_code,split = "-",type.convert = TRUE)]

resi_cust_info <- resi_cust_info[!(gsub(" ","",postal1) %in% c("72122","71909","72167","72065"))]
nrow(resi_cust_info)

setkey(resi_cust_info,area,business_unit,lawson_company,
       infopro_div_nbr,account_number,container_grp)

resi_cust_info_sub <- unique(copy(resi_cust_info)[,list(area,business_unit,lawson_company,
                                                        infopro_div_nbr,account_number,
                                                        site_nbr=substr(container_grp,11,15),
                                                        cont_grp=substr(container_grp,16,17))])
nrow(resi_cust_info_sub)

setkey(resi_cust_info,area,business_unit,lawson_company,
       infopro_div_nbr,account_number)

resi_cust_info_sub_email <- unique(copy(resi_cust_info)[,list(area,business_unit,lawson_company,
                                                              infopro_div_nbr,account_number,
                                                              email,account_open_date)])
nrow(resi_cust_info_sub_email)
setkey(resi_cust_info_sub,area,business_unit,lawson_company,
       infopro_div_nbr,account_number)

setkey(resi_cust_info_sub_email,area,business_unit,lawson_company,
       infopro_div_nbr,account_number)

resi_cust_info_sub <- resi_cust_info_sub[resi_cust_info_sub_email]
nrow(resi_cust_info_sub)
#Removing F2 notes customers
F2notecust <- data.table(sqlProc(server="prodbirpt", db="DWPSA",
                                 sqlFile="SELECT [SUCOMP]
                                  ,[SUACCT]
                                 ,[SUSITE]
                                 FROM [DWPSA].[dbo].[STG_IFP_BIPSUC]
                                 where [SUSCOD] = 'CSCF' 
                                 and (SUsCDT>=20160701)
                                 --and (SUCDaT>20160701)
                                 and SUCOMP in (894, 899, 923, 858, 282, 320, 865)
                                 and CURRENT_IND = 'Y'",
                                 asText=TRUE,windows_auth = TRUE, as.is = TRUE))
F2notecust <- unique(F2notecust)

F2notecust[,':='(F2flag='Y')]


F2notecust[,`:=`(sucomp= sprintf("%03.f", as.numeric(sucomp)),
                                        suacct = sprintf("%07.f", as.numeric(suacct)),
                  susite=sprintf("%05.f", as.numeric(susite)))]

F2notecust[,`:=`(suacct = paste0(sucomp, suacct))]


resi_cust_info_sub[,`:=`(infopro_div_nbr= sprintf("%03.f", as.numeric(infopro_div_nbr)),
                 account_number = sprintf("%010.f", as.numeric(account_number)),
                 site_nbr=sprintf("%05.f", as.numeric(site_nbr)))]

setkey(resi_cust_info_sub,infopro_div_nbr,account_number,site_nbr)

setkey(F2notecust, sucomp, suacct, susite)
nrow(F2notecust)
nrow(resi_cust_info_sub)
resi_cust_info_sub <- F2notecust[resi_cust_info_sub]

resi_cust_info_sub <- resi_cust_info_sub[is.na(F2flag)]
nrow(resi_cust_info_sub)
resi_cust_info_sub[,account_open_date:=as.numeric(gsub("-","",account_open_date))]

setnames(resi_cust_info_sub, "sucomp", "infopro_div_nbr")
setnames(resi_cust_info_sub, "suacct", "account_number")
setnames(resi_cust_info_sub, "susite", "site_nbr")

resi_cust_info_sub[, F2flag:=NULL]
setcolorder(resi_cust_info_sub,c("area","business_unit","lawson_company","infopro_div_nbr","account_number",
                                 "site_nbr","cont_grp","email","account_open_date"))

#Contract Group Number and close date

contract_grp <- data.table(sqlProc(server="prodbirpt", db="DWCORE",
                                   sqlFile="SELECT Infopro_Div_Nbr,Acct_Nbr,Site_Nbr,Container_Grp_Nbr,
                                   Revenue_Distribution_Cd,Contract_Grp_Nbr,Acct_Type,Orig_Start_Dt,Rate_Restrict_Dt,
                                   Rate_Type,Stop_Cd,District_Cd,Recurring_Charge_Freq,Seasonal_Stop_Dt,
                                   Seasonal_Restart_Dt,Close_Dt,Disposal_Rate_Restrict_Dt,is_Active,Contract_Nbr,
                                   Pickup_Period_Total_Lifts,Pickup_Period_Length,Pickup_Period_Unit,Container_Cnt,Container_Cd
                                   FROM Dim_Container_Grp where CURRENT_TIMESTAMP between Eff_Dt and Disc_Dt 
                                   AND is_Current = '1' and Acct_Type NOT in ('X','T') 
                                   AND Contract_Grp_Nbr = '0'  and Infopro_Div_Nbr in ('858','894','899','923', '282', '320', '865')",
                                   asText=TRUE,windows_auth = TRUE, as.is = TRUE))
#AND is_Active = '1'

##Gaurav explain why this condition was included...appears to be excluding accounts we dont want excluded
#and Close_Dt > CURRENT_TIMESTAMP 
contract_grp[,`:=`(acct_nbr=sprintf("%07.f", as.numeric(acct_nbr)),
                   infopro_div_nbr=sprintf("%03.f", as.numeric(infopro_div_nbr)),
                   cont_grp=sprintf("%02.f", as.numeric(container_grp_nbr)))]

contract_grp_sub <- contract_grp[pickup_period_total_lifts != 0 & pickup_period_length != 0 & 
                                   pickup_period_unit != "UNKNOWN"
                                 ,list(infopro_div_nbr,
                                       account_number=paste0(infopro_div_nbr,acct_nbr),
                                       site_nbr,
                                       cont_grp,
                                       lifts_per_week=as.numeric(pickup_period_total_lifts)/
                                         as.numeric(pickup_period_length),
                                       quantity=as.numeric(container_cnt),
                                       container_cd,
                                       RCF=as.numeric(recurring_charge_freq),
                                       close_date=as.numeric(paste0(substr(close_dt,1,4),
                                                                    substr(close_dt,6,7),
                                                                    substr(close_dt,9,10))))]

revenue_grp <- data.table(sqlProc(server="prodbirpt", db="DWPSA",
                                  sqlFile="SELECT COMPANY_NUMBER AS infopro_div_nbr,ACCOUNT_NUMBER,SITE AS site_nbr,
                                  CONTAINER_GROUP AS container_grp_nbr,CHARGE_RATE AS revenue,CHARGE_METHOD 
                                  FROM (SELECT *, Row_number() 
                                  OVER ( partition BY ac.COMPANY_NUMBER, ac.ACCOUNT_NUMBER, ac.SITE,ac.CONTAINER_GROUP
                                  ORDER BY ac.DM_TIMESTAMP DESC) AS rn 
                                  FROM STG_IFP_BIPCCR AS ac
                                  WHERE CHARGE_TYPE = 'R' AND
                                  COMPANY_NUMBER in ('858','894','899','923','282', '320', '865')
                                  AND ETL_BATCH_ID is not null) AS z
                                  WHERE rn = 1",
                                  asText=TRUE,windows_auth = TRUE, as.is = TRUE))

revenue_grp[,`:=`(account_number=paste0(sprintf("%03.f", as.numeric(infopro_div_nbr)),
                                        sprintf("%07.f", as.numeric(account_number))),
                  infopro_div_nbr=sprintf("%03.f", as.numeric(infopro_div_nbr)),
                  cont_grp=sprintf("%02.f", as.numeric(container_grp_nbr)))]

revenue_grp[,container_grp_nbr:=NULL]

str(revenue_grp)
str(contract_grp_sub)
nrow(revenue_grp)
nrow(contract_grp_sub)
setkey(revenue_grp,infopro_div_nbr,account_number,site_nbr,cont_grp)

setkey(contract_grp_sub,infopro_div_nbr,account_number,site_nbr,cont_grp)

contract_grp_sub <- merge(contract_grp_sub,revenue_grp, all.x=TRUE)
nrow(contract_grp_sub)
contract_grp_sub[,revenue:=ifelse(charge_method=="Q",as.numeric(revenue)*as.numeric(quantity)*RCF,as.numeric(revenue)*RCF)]

#contract_grp_sub[,RCF:=NULL]
nrow(resi_cust_info_sub )
contract_grp_sub <- contract_grp_sub[paste0(infopro_div_nbr,account_number,site_nbr,cont_grp) %in% 
                                       paste0(resi_cust_info_sub$infopro_div_nbr,
                                              resi_cust_info_sub$account_number,resi_cust_info_sub$site_nbr,
                                             resi_cust_info_sub$cont_grp)]
nrow(contract_grp_sub)
contract_grp_sub<- contract_grp_sub[close_date> 20160627]
nrow(resi_cust_info_sub)

contract_grp_sub_invalid <- contract_grp_sub[is.na(revenue) | revenue <=0]
nrow(contract_grp_sub_invalid)
nrow(contract_grp_sub[is.na(revenue)])
nrow(contract_grp_sub[revenue<=0])
# revlessthan0 <- nrow(contract_grp_sub_invalid)

contract_grp_sub <- contract_grp_sub[!is.na(revenue) & revenue > 0]
nrow(contract_grp_sub)
contract_grp_sub2 <- copy(contract_grp_sub)

contract_grp_sub2[,`:=`(account_site= paste0(account_number,site_nbr))]

keepCols <- c("account_site")

contract_grp_sub2 <- contract_grp_sub2[,keepCols,with=FALSE]

contract_grp_sub2 <- unique(contract_grp_sub2)
nrow(contract_grp_sub2)
contract_grp_sub2[,`:=`(account_number= substr(account_site, 1, 10),account_site=NULL)]

contract_grp_sub2[,count:= .N,by=list(account_number)]

contract_grp_sub2 <- contract_grp_sub2[ , max(count), by = account_number]

morethan3sites <- nrow(contract_grp_sub2[V1 > 3])

contract_grp_sub2 <- contract_grp_sub2[V1 <= 3]
nrow(contract_grp_sub)
contract_grp_sub <- contract_grp_sub[account_number %in% contract_grp_sub2$account_number]
nrow(contract_grp_sub)



contract_grp_sub_lifts <- contract_grp_sub[,list(infopro_div_nbr,account_number,lifts_per_week,revenue)]

contract_grp_sub_date <- contract_grp_sub[,list(infopro_div_nbr,account_number,container_cd,close_date)]

sumByCols <- c("infopro_div_nbr","account_number")

contract_grp_sub_lifts <- contract_grp_sub_lifts[,lapply(.SD,sum),by=sumByCols,.SDcols=c("lifts_per_week","revenue")]

contract_grp_sub_lifts[,rev_per_lift:=revenue/(lifts_per_week*52)]

setnames(contract_grp_sub_lifts, "revenue", "base_revenue")
setnames(contract_grp_sub_lifts, "rev_per_lift", "base_rev_per_lift")

#contract_grp_sub_lifts[,`:=`(revenue=NULL)]

contract_grp_sub_date[,`:=`(waste_rcy=ifelse(container_cd %in% c("FR","BB","RR","IR","RC","AS","AN","AO"),
                                             "RCY",NA_character_),
                            waste_msw=ifelse(!(container_cd %in% c("FR","BB","RR","IR","RC","AS","AN","AO","YC","YW")),
                                             "SW",NA_character_),
                            waste_yw=ifelse(container_cd %in% c("YC","YW"),"YW",NA_character_),
                            container_cd=NULL)]

contract_grp_sub_date_rcy <- copy(contract_grp_sub_date)[waste_rcy=="RCY"]

contract_grp_sub_date_rcy[,`:=`(close_date=NULL,waste_msw=NULL,waste_yw=NULL)]

contract_grp_sub_date_msw <- copy(contract_grp_sub_date)[waste_msw=="SW"]

contract_grp_sub_date_msw[,`:=`(close_date=NULL,waste_rcy=NULL,waste_yw=NULL)]

contract_grp_sub_date_yw <- copy(contract_grp_sub_date)[waste_yw=="YW"]

contract_grp_sub_date_yw[,`:=`(close_date=NULL,waste_rcy=NULL,waste_msw=NULL)]

contract_grp_sub_date[,`:=`(waste_rcy=NULL,waste_msw=NULL,waste_yw=NULL)]

setkey(contract_grp_sub_date_rcy,infopro_div_nbr,account_number)

setkey(contract_grp_sub_date_msw,infopro_div_nbr,account_number)

setkey(contract_grp_sub_date_yw,infopro_div_nbr,account_number)

contract_grp_sub_date_rcy <- unique(contract_grp_sub_date_rcy)

contract_grp_sub_date_msw <- unique(contract_grp_sub_date_msw)

contract_grp_sub_date_yw <- unique(contract_grp_sub_date_yw)

contract_grp_sub_date_msw_rcy <- merge(contract_grp_sub_date_msw,contract_grp_sub_date_rcy,all=TRUE)

setkey(contract_grp_sub_date_msw_rcy,infopro_div_nbr,account_number)

contract_grp_sub_date_msw_rcy_yw <- merge(contract_grp_sub_date_msw_rcy,contract_grp_sub_date_yw,all=TRUE)

contract_grp_sub_date_msw_rcy_yw[,`:=`(waste_category=ifelse(!is.na(waste_msw) & !is.na(waste_rcy) & !is.na(waste_yw),"SW/RCY/YW",
                                                             ifelse(!is.na(waste_msw) & !is.na(waste_rcy) & is.na(waste_yw),"SW/RCY",
                                                                    ifelse(!is.na(waste_msw) & is.na(waste_rcy) & !is.na(waste_yw),"SW/YW",
                                                                           ifelse(is.na(waste_msw) & !is.na(waste_rcy) & !is.na(waste_yw),"RCY/YW",
                                                                                  ifelse(!is.na(waste_msw) & is.na(waste_rcy) & is.na(waste_yw),"SW",
                                                                                         ifelse(is.na(waste_msw) & !is.na(waste_rcy) & is.na(waste_yw),"RCY",
                                                                                                ifelse(is.na(waste_msw) & is.na(waste_rcy) & !is.na(waste_yw),"YW",
                                                                                                       NA_character_))))))),
                                       waste_msw=NULL,waste_rcy=NULL,waste_yw=NULL)]

setkey(contract_grp_sub_date,infopro_div_nbr,account_number)
setkey(contract_grp_sub_date_msw_rcy_yw,infopro_div_nbr,account_number)

contract_grp_sub_date <- contract_grp_sub_date_msw_rcy_yw[contract_grp_sub_date]

setkey(contract_grp_sub_date,infopro_div_nbr,account_number,waste_category)

contract_grp_sub_date <- contract_grp_sub_date[ , max(close_date), by = key(contract_grp_sub_date)]

setnames(contract_grp_sub_date,"V1","close_date")

setkey(contract_grp_sub_lifts,infopro_div_nbr,account_number)
setkey(contract_grp_sub_date,infopro_div_nbr,account_number)

contract_grp_subl <- contract_grp_sub_lifts[contract_grp_sub_date]
nrow(resi_cust_info_sub)
resi_cust_info_subset <- unique(copy(resi_cust_info_sub)[,list(area,business_unit,lawson_company,
                                                               infopro_div_nbr,account_number,
                                                               email,account_open_date)],
                                by=c("area","business_unit","lawson_company",
                                     "infopro_div_nbr","account_number"))

nrow(resi_cust_info_subset)

nrow(contract_grp_subl)
contract_grp_sub <- unique(contract_grp_sub[, list(infopro_div_nbr, account_number, RCF)], by = c("infopro_div_nbr", "account_number"))
nrow(contract_grp_sub)
setkey(contract_grp_subl,infopro_div_nbr,account_number)
setkey(resi_cust_info_subset,infopro_div_nbr,account_number)

nrow(resi_cust_info_subset)
nrow(contract_grp_subl)
nrow(contract_grp_sub)

resi_cust_info_sub2 <- merge(resi_cust_info_subset,contract_grp_subl)
nrow(resi_cust_info_sub2)
setkey(contract_grp_sub, infopro_div_nbr, account_number)
setkey(resi_cust_info_sub2, infopro_div_nbr,account_number)
nrow(resi_cust_info_sub2)
resi_cust_info_sub2 <- merge(resi_cust_info_sub2,contract_grp_sub)
nrow(resi_cust_info_sub2)

# NRD

Next_review_dt <- data.table(sqlProc(server="prodbirpt", db="DWPSA",
                                     sqlFile="SELECT ifcomp, ifacct, CONVERT(DATETIME, CONVERT(CHAR(8), ifdat1)) 
                                     AS next_review_dt FROM (SELECT ifcomp, ifacct,
                                     case when ifdat1 > 30000000 or ifdat1 < 20000000 
                                     or right(ifdat1,2) * 1 < 1 or right(ifdat1,2) * 1 > 31
                                     or left(right(ifdat1,4),2) * 1 < 1
                                     or left(right(ifdat1,4),2) * 1 > 12
                                     then NULL else ifdat1 end as ifdat1, 
                                     Row_number() 
                                     OVER ( 
                                     partition BY ac.ifcomp, ac.ifacct 
                                     ORDER BY ac.dm_timestamp DESC) AS rn 
                                     FROM stg_ifp_bipif AS ac 
                                     WHERE ac.IFCOMP in ('858','894','899','923', '282', '320', '865')
                                     AND ETL_BATCH_ID is not null ) AS z 
                                     WHERE  rn = 1 and CONVERT(DATETIME, CONVERT(CHAR(8), ifdat1)) IS NOT NULL",
                                     asText=TRUE,windows_auth = TRUE, as.is = TRUE))


Next_review_dt[,`:=`(ifacct=sprintf("%07.f", as.numeric(ifacct)),
                     ifcomp=sprintf("%03.f", as.numeric(ifcomp)))]

next_review_dt_sub <- unique(Next_review_dt[,list(infopro_div_nbr=ifcomp,
                                                  account_number=paste0(ifcomp,ifacct),
                                                  NRD=as.numeric(paste0(substr(next_review_dt,1,4),
                                                                        substr(next_review_dt,6,7),
                                                                        substr(next_review_dt,9,10))))])

setkey(next_review_dt_sub,infopro_div_nbr,account_number)
#View(next_review_dt_sub[duplicated(next_review_dt_sub)])
nrow(resi_cust_info_sub2)
resi_cust_info_sub3 <- merge(resi_cust_info_sub2,next_review_dt_sub)
nrow(resi_cust_info_sub3)
resi_cust_info_sub3[,`:=`(tenure_years=as.numeric(Sys.Date() - as.Date(as.character(account_open_date),"%Y%m%d"))/365,
                          tenure_months=as.numeric(Sys.Date() - as.Date(as.character(account_open_date),"%Y%m%d"))/(365/12))]


fee_flags <- data.table(sqlProc(server="prodbirpt", db="DWCORE",
                                sqlFile="SELECT Infopro_Div_Nbr,Fee_Type,is_Fee_Charged,Acct_Nbr
                                FROM Dim_Acct_Fee where CURRENT_TIMESTAMP between Eff_Dt and Disc_Dt and
                                Fee_Type in ('FRF','EVR','INV')",
                                asText=TRUE,windows_auth = TRUE, as.is = TRUE))

fee_flag_rates <- data.table(sqlProc(server="prodbmisql01", db="BMIDM",
                  sqlFile = "SELECT Infopro_Div_Nbr, frf_rate, erf_rate, admin_fee, erf_on_frf
                  FROM [dbo].[divisionFeeRate]",
                  asText = TRUE, windows_auth = TRUE, as.is= TRUE))

fee_flags <- fee_flags[infopro_div_nbr %in% as.character(as.numeric(resi_cust_info_sub3$infopro_div_nbr))]

fee_flags[,`:=`(infopro_div_nbr=sprintf("%03.f", as.numeric(infopro_div_nbr)),
                account_number=paste0(sprintf("%03.f", as.numeric(infopro_div_nbr)),
                                      sprintf("%07.f", as.numeric(acct_nbr))))]

fee_flags[,`:=`(infopro_div_nbr=NULL,acct_nbr=NULL)]

fee_flags <- dcast.data.table(fee_flags,account_number ~ fee_type,value.var = "is_fee_charged")

setkey(fee_flags,account_number)
setkey(resi_cust_info_sub3,account_number)
nrow(resi_cust_info_sub3)
resi_cust_info_sub3 <- fee_flags[resi_cust_info_sub3]
nrow(resi_cust_info_sub3)

setkey(fee_flag_rates,infopro_div_nbr)
setkey(resi_cust_info_sub3,infopro_div_nbr)
nrow(resi_cust_info_sub3)
resi_cust_info_sub3 <- fee_flag_rates[resi_cust_info_sub3]
nrow(resi_cust_info_sub3)
#Calculating ERF FRF and Admin Fees and adding to Revenue, creating column total revenue.
resi_cust_info_sub3[,`:=`(admin_fee = as.numeric(admin_fee)/RCF)]
resi_cust_info_sub3[,`:=`(Base_Charge= (base_revenue+as.numeric(admin_fee)*(as.numeric(INV))))]
resi_cust_info_sub3[,`:=`(FRF_Charge= ((as.numeric(FRF))*Base_Charge*(as.numeric(frf_rate)/100)))]
resi_cust_info_sub3[,`:=`(ERF_Charge= ((as.numeric(EVR)*as.numeric(erf_rate)/100)*(Base_Charge + as.numeric(erf_on_frf) * FRF_Charge)))]

resi_cust_info_sub3[,`:=`(revenue = Base_Charge+FRF_Charge+ERF_Charge )]

resi_cust_info_sub3[,rev_per_lift:=revenue/(lifts_per_week*52)]

resi_cust_info_sub3[, `:=`(frf_rate= NULL, erf_rate= NULL, admin_fee=NULL, EVR=NULL, FRF=NULL, INV=NULL, erf_on_frf=NULL, RCF=NULL)]


setcolorder(resi_cust_info_sub3,c("infopro_div_nbr","account_number","area","business_unit","lawson_company",
                                  "email","account_open_date","base_revenue" , "base_rev_per_lift","lifts_per_week","waste_category",
                                  "close_date","NRD" ,"tenure_years","tenure_months", "Base_Charge", "FRF_Charge", "ERF_Charge", "revenue", "rev_per_lift"))



lessthan18months <- nrow(copy(resi_cust_info_sub3)[tenure_months <= 18])
lessthan18months
nrow(resi_cust_info_sub3)
resi_cust_info_sub4 <- copy(resi_cust_info_sub3)[tenure_months > 18]
nrow(resi_cust_info_sub4)
resi_cust_info_sub4[,`:=`(GOE=ifelse(infopro_div_nbr=="923",1.76,ifelse(infopro_div_nbr=="858",1.48,
                                                                        ifelse(infopro_div_nbr=="899",1.29,
                                                                               ifelse(infopro_div_nbr=="894",1.26,
                                                                                      ifelse(infopro_div_nbr=="282",1.01,
                                                                                              ifelse(infopro_div_nbr=="320",1.54,
                                                                                                      ifelse(infopro_div_nbr=="865",1.92,0))))))),
                          depr=ifelse(infopro_div_nbr=="923",0.24,ifelse(infopro_div_nbr=="858",0.46,
                                                                         ifelse(infopro_div_nbr=="899",0.30,
                                                                                ifelse(infopro_div_nbr=="894",0.31,
                                                                                       ifelse(infopro_div_nbr=="282",0.12,
                                                                                              ifelse(infopro_div_nbr=="320",0.24,
                                                                                                     ifelse(infopro_div_nbr=="865",0.33,0))))))))]

resi_cust_info_sub4[,profitability:=rev_per_lift-GOE-depr]

percentile_test <- copy(resi_cust_info_sub4)

setkey(percentile_test,profitability)

#View(percentile_test)
nrow(percentile_test)
percentile_test <- percentile_test[!is.na(profitability) & profitability > 0]
nrow(percentile_test)
percentile_test[,global_rank:=round(as.numeric(gsub("%","",names(quantile(profitability, probs = seq(0, 1, by= 1/(nrow(percentile_test)-1)))))),2)]

rm(list=setdiff(ls(), c("percentile_test","resi_cust_info_sub3","resi_cust_info_sub2","resi_cust_info_sub")))
##Stratification

percentile_test[,cat_tenure:=ifelse(tenure_years <=3,"less than 3 yrs",
                                    ifelse(tenure_years < 6,"3 to 6 years","greater than 6 yrs"))]

setkey(percentile_test,infopro_div_nbr,cat_tenure)

percentile_test[,local_rank:=round(as.numeric(gsub("%","",names(quantile(profitability, probs = seq(0, 1, by= 1/(.N-1)))))),2),by=list(infopro_div_nbr,cat_tenure)]

percentile_test[,`:=`(cat_rank=ifelse(local_rank <= 25,"0-25%",ifelse(local_rank <= 75,"25-75%","75-100%")))]
nrow(percentile_test)
percentile_test <- percentile_test[!(gsub(" ","",waste_category) %in% c("SW/YW","YW","RCY/YW","SW/RCY/YW"))]
nrow(percentile_test)

percentile_test[,.N,by=list(infopro_div_nbr, cat_tenure, cat_rank, waste_category)]



percentile_test[,grp:=runif(.N,0,1),by=list(infopro_div_nbr, cat_tenure, cat_rank, waste_category)]

percentile_test[,grp2:=ifelse(grp >= 0.8,"HO",
                              ifelse((grp >= 0.55 & grp != "HO"),"A11",
                                                    ifelse(grp >= 0.3 & grp !="A11" & grp !="HO", "A12",
                                                           ifelse(grp >=0.15 & grp!="A11" & grp!= "HO"& grp!= "A12", "A21",
                                                                  "A22"
                                                           ))))]

# percentile_test[,list(HO=sum(ifelse(grp2=="HO",1.0,0.0))/.N,
#                A1=sum(ifelse(grp2=="A1",1.0,0.0))/.N,
#                A2=sum(ifelse(grp2=="A2",1.0,0.0))/.N),by=list(infopro_div_nbr, cat_tenure, cat_rank, waste_category)]


nrow(percentile_test)
write.table(percentile_test,paste0("D:/Analytics/Resi Email Retention/email_ret_data1.csv"),
            sep=",",na="",row.names=FALSE)

