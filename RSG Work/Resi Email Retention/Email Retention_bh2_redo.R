library(data.table)
library(RODBC)
library(xlsx)
library(stringr)
library(rsgutilr)

# Universe <- data.table(sqlProc(server="prodbirpt", db="DWCORE",
#                                sqlFile="SELECT * FROM RPT_CUST_MKTG_INFO Where Customer_Category = 'Residential' 
#                                and Franchise = 'N' and National_Acct = 'N' and Acct_Type != 'Temp'",
#                                asText=TRUE,windows_auth = TRUE, as.is = TRUE))
# 
# print_var <- paste0("1. Intial Pull from RPT_CUST_MKTG_INFO non franchise, non national and perm accounts are ",
#                     nrow(Universe),
#                     " rows (container group level)")
# rm(Universe)

resi_cust_info <- data.table(sqlProc(server="prodbirpt", db="DWCORE",
                                     sqlFile="SELECT * FROM RPT_CUST_MKTG_INFO Where Customer_Category = 'Residential' 
                                     and Email != '' and Franchise = 'N' and National_Acct = 'N' and Acct_Type != 'Temp'",
                                     asText=TRUE,windows_auth = TRUE, as.is = TRUE))

print_var <- paste0("\n", "2. Intial Pull from RPT_CUST_MKTG_INFO with email column populated, non franchise, non national and perm accounts are ",
                    nrow(resi_cust_info),
                    " rows (container group level)")

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

resi_cust_info[,c("postal1","postal2"):=tstrsplit(postal_code,split = "-",type.convert = TRUE)]


setkey(resi_cust_info,area,business_unit,lawson_company,
       infopro_div_nbr,account_number,container_grp)

resi_cust_info_sub <- unique(copy(resi_cust_info)[,list(area,business_unit,lawson_company,
                                                        infopro_div_nbr,account_number,
                                                        site_nbr=substr(container_grp,11,15),
                                                        cont_grp=substr(container_grp,16,17))])

print_var <- paste0(print_var,"\n","8. The records after Unique on Infopro, Account Number, Site NUmber and Container Group: ",nrow(resi_cust_info_sub))

dba_custserviceno <- data.table(sqlProc(server="qabmisql01", db="BMIDM",
                                        sqlFile="SELECT 
                                        distinct a.[division] as Lawson_company
                                        , a.[support_phone] as Customer_Service_Number
                                        , b.[dba_name] as Doing_Business_As
                                        FROM [BMIDM].[dbo].[lawson_division] a join [BMIDM].[dbo].[div_dba_names] b 
                                        on a.division = b.division_nbr
                                        where 
                                        CURRENT_TIMESTAMP between a.eff_dt and a.disc_dt
                                        and b.is_legal_entity_name = 1
                                        and division in (3282, 4320, 4858, 3211, 4894, 4899, 4923)
                                        ",
                                        asText=TRUE, windows_auth = TRUE, as.is = TRUE))

setkey(dba_custserviceno, lawson_company)
setkey(resi_cust_info, lawson_company)

resi_cust_info <- dba_custserviceno[resi_cust_info]



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
                                                              , telephone_number_1
                                                              , customer_service_number
                                                              , doing_business_as)])
nrow(resi_cust_info_sub_email)
setkey(resi_cust_info_sub,area,business_unit,lawson_company,
       infopro_div_nbr,account_number)

setkey(resi_cust_info_sub_email,area,business_unit,lawson_company,
       infopro_div_nbr,account_number)

resi_cust_info_sub <- resi_cust_info_sub[resi_cust_info_sub_email]
nrow(resi_cust_info_sub)


resi_cust_info_sub[,`:=`(infopro_div_nbr= sprintf("%03.f", as.numeric(infopro_div_nbr)),
                         account_number = sprintf("%010.f", as.numeric(account_number)),
                         site_nbr=sprintf("%05.f", as.numeric(site_nbr)))]

setkey(resi_cust_info_sub,infopro_div_nbr,account_number,site_nbr)



resi_cust_info_sub <- resi_cust_info_sub[is.na(F2flag)]
nrow(resi_cust_info_sub)
resi_cust_info_sub[,account_open_date:=as.numeric(gsub("-","",account_open_date))]

#setnames(resi_cust_info_sub, "sucomp", "infopro_div_nbr")
#setnames(resi_cust_info_sub, "suacct", "account_number")
#setnames(resi_cust_info_sub, "susite", "site_nbr")

resi_cust_info_sub[, F2flag:=NULL]


#Contract Group Number and close date and revenue

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
                                   b.COMPANY_NUMBER in ('858','894','899','923','282', '320', '865')
                                   and  (b.CLOSE_DATE>20160701 or b.close_date = 0) 
                                   and 	b.EFFECTIVE_DATE<=20160701
                                   and b.current_ind='Y'
                                   and dcg.Acct_Type NOT in ('X','T') 
                                   AND dcg.Contract_Grp_Nbr = '0'  
                                   and  ds.close_dt > '2017-06-30' )
                                   select
                                   addr_line_1,addr_line_2 ,city ,state ,postal_cd,postal_cd_plus4
                                   ,infopro_div_nbr, acct_nbr, site_nbr, container_grp_nbr
                                   ,Revenue_Distribution_Cd, Contract_Grp_Nbr, Acct_Type, Orig_Start_Dt
                                   ,Rate_Restrict_Dt, Rate_Type,Stop_Cd
                                   ,District_Cd,Recurring_Charge_Freq
                                   ,Seasonal_Stop_Dt,Seasonal_Restart_Dt
                                   ,Close_Dt,Disposal_Rate_Restrict_Dt
                                   ,is_Active,Contract_Nbr
                                   ,Pickup_Period_Total_Lifts
                                   ,Pickup_Period_Length
                                   ,Pickup_Period_Unit,Container_Cnt,Container_Cd
                                   ,max(dup_cc) as dup_cc
                                   ,sum(revenue_ann) as revenue_annual
                                   ,sum(revenue_per_bill) as revenue_per_bill
                                   
                                   from accts 
                                   group by 
                                   infopro_div_nbr, acct_nbr, site_nbr, container_grp_nbr
                                   ,Revenue_Distribution_Cd, Contract_Grp_Nbr, Acct_Type, Orig_Start_Dt
                                   ,Rate_Restrict_Dt, Rate_Type,Stop_Cd
                                   ,District_Cd,Recurring_Charge_Freq
                                   ,Seasonal_Stop_Dt,Seasonal_Restart_Dt
                                   ,Close_Dt,Disposal_Rate_Restrict_Dt
                                   ,is_Active,Contract_Nbr
                                   ,Pickup_Period_Total_Lifts
                                   ,Pickup_Period_Length
                                   ,Pickup_Period_Unit,Container_Cnt,Container_Cd
                                   ,addr_line_1,addr_line_2 ,city ,state ,postal_cd,postal_cd_plus4
                                   ",
                                   asText=TRUE,windows_auth = TRUE, as.is = TRUE))


contract_grp[,`:=`(acct_nbr=paste0(sprintf("%03.f", as.numeric(infopro_div_nbr)),
                                   sprintf("%07.f", as.numeric(acct_nbr)) ),
                   infopro_div_nbr=sprintf("%03.f", as.numeric(infopro_div_nbr)),
                   cont_grp=sprintf("%02.f", as.numeric(container_grp_nbr)))]

contract_grp[, `:=`(Service_Address = paste0(addr_line_1 , ", ", addr_line_2))]
contract_grp[, `:=`(Service_Zip = paste0(postal_cd, "-", postal_cd_plus4))]

contract_grp[, `:=` (addr_line_1=NULL, addr_line_2=NULL, postal_cd=NULL, postal_cd_plus4= NULL )]

ServiceAddr <- unique(contract_grp[, list(acct_nbr, Service_Address, city, state, Service_Zip)], by= "acct_nbr")


setnames(contract_grp, "recurring_charge_freq", "RCF")

print_var <- paste0(print_var,"\n","12. Data pulled in from STG_IFP_BIPCCR and Dim_Container_Grp pulled in Datatable COntract_Grp- ",nrow(contract_grp))


################# Removing customers with 91 as District Code in SRD  #################
contract_grp<- contract_grp[!district_cd == '91']


#add var for number of sites
contract_grp$num_sites<-1
numsites<-contract_grp[,list(num_cg=sum(num_sites)), by=list(infopro_div_nbr, acct_nbr, site_nbr)]

numsites[,list(num=length(acct_nbr)), by=num_cg]
numsites<-numsites[,list(num_sites=length(num_cg)), by=list(infopro_div_nbr, acct_nbr)]

nrow(numsites)
numsites[,list(num=length(acct_nbr)), by=num_sites]
#create inclusion list ..then merge back
lt3sites<-numsites[num_sites<=3,]
setkeyv(lt3sites, c("infopro_div_nbr","acct_nbr"))
setkeyv(contract_grp, c("infopro_div_nbr","acct_nbr"))
nrow(contract_grp)
contract_grp<-merge(contract_grp, lt3sites)
nrow(contract_grp)
print_var <- paste0(print_var,"\n","13. Eliminating more than 3 sites from contract_grp ",nrow(contract_grp))

#Calculating ERF FRF and Admin Fees and adding to Revenue, creating column total revenue.
# contract_grp[,`:=`(revenue_annual= (as.numeric(revenue_annual)+as.numeric(admin_fee)*(as.numeric(INV))))]
# 
# contract_grp[,`:=`(admin_fee = as.numeric(admin_fee)/RCF)]
# 
# contract_grp[,`:=`(revenue_per_bill= (as.numeric(revenue_per_bill)+as.numeric(admin_fee)*(as.numeric(INV))))]
# 
# contract_grp[, `:=`(frf_rate= NULL, erf_rate= NULL, admin_fee=NULL, EVR=NULL, FRF=NULL, INV=NULL, erf_on_frf=NULL)]


contract_grp_sub <- contract_grp[pickup_period_total_lifts != 0 & pickup_period_length != 0 & 
                                   pickup_period_unit != "UNKNOWN"
                                 ,list(infopro_div_nbr,
                                       acct_nbr,
                                       site_nbr,
                                       cont_grp,
                                       lifts_per_week=as.numeric(pickup_period_total_lifts)/
                                         as.numeric(pickup_period_length),
                                       quantity=as.numeric(container_cnt),
                                       container_cd,
                                       RCF=as.numeric(RCF),
                                       close_date=as.numeric(paste0(substr(close_dt,1,4),
                                                                    substr(close_dt,6,7),
                                                                    substr(close_dt,9,10))),
                                       revenue_per_bill=as.numeric(revenue_per_bill),
                                       revenue_annual=as.numeric(revenue_annual))]


str(contract_grp_sub)

#contract_grp_sub[,RCF:=NULL]
##elim closed containers

contract_grp_sub<- contract_grp_sub[close_date> 20160707]

print_var <- paste0(print_var,"\n","14. Eliminating container Closed  as of 7 July, 2016 ",nrow(contract_grp_sub))

contract_grp_sub_invalid <- contract_grp_sub[is.na(revenue_per_bill) | revenue_per_bill <=0]

print_var <- paste0(print_var,"\n","15. Negative and Is. NA revenue records-  ",nrow(contract_grp_sub_invalid))

nrow(contract_grp_sub_invalid)
nrow(contract_grp_sub[is.na(revenue_per_bill)])
nrow(contract_grp_sub[revenue_per_bill<0])
nrow(contract_grp_sub[revenue_per_bill==0])
# revlessthan0 <- nrow(contract_grp_sub_invalid)

#rev exclusion

print_var <- paste0(print_var,"\n","16. Rows left after eliminating negative and is.na revenue-  ",nrow(contract_grp_sub))

# contract_grp_sub2 <- copy(contract_grp_sub)
# 
# contract_grp_sub2[,`:=`(account_site= paste0(account_number,site_nbr ))]
# 
# keepCols <- c("account_site")

#contract_grp_sub2 <- contract_grp_sub2[,keepCols,with=FALSE]

#contract_grp_sub2 <- unique(contract_grp_sub2)

#contract_grp_sub2[,`:=`(account_number= substr(account_site, 1, 10),account_site=NULL)]

#contract_grp_sub2[,count:= .N,by=list(account_number)]

#contract_grp_sub2 <- contract_grp_sub2[ , max(count), by = account_number]

#morethan3sites <- nrow(contract_grp_sub2[V1 > 3])

#contract_grp_sub2 <- contract_grp_sub2[V1 <= 3]
#nrow(contract_grp_sub)
#contract_grp_sub <- contract_grp_sub[account_number %in% contract_grp_sub2$account_number]
#nrow(contract_grp_sub)

rate_upload <- copy(contract_grp_sub)
save(rate_upload,file="//prodpricapp02/d/Analytics/Resi Email Retention/rate_upload.robj")
#load("//prodpricapp02/d/Analytics/Resi Email Retention/rate_upload.robj")
########## Start: Calculating Max of RCF if more than one RCF for multiple sites/ container groups#########
nrow(contract_grp_sub)
tempcheck <- contract_grp_sub[, list(acct_nbr, site_nbr, RCF)]
# tempcheck[,count1:= .N,by=list( acct_nbr, site_nbr)]
# tempcheck[,count2:= .N,by=list(acct_nbr)]
tempchecl <- tempcheck[, lapply(.SD[which.max(RCF)], max), by = c("acct_nbr")]

#tempchecl[,`:=`(count1= NULL, count2=NULL, site_nbr= NULL)]
contract_grp_sub [, `:=` (RCF= NULL)]
setkey(contract_grp_sub, acct_nbr)
setkey(tempchecl, acct_nbr)

contract_grp_sub<-  merge(contract_grp_sub, tempchecl)

print_var <- paste0(print_var,"\n","17. Records after doing max of RCF if there are multiple sites-  ",nrow(contract_grp_sub))

nrow(contract_grp_sub)

########## End: Calculating Max of RCF if more than one RCF for multiple sites/ container groups#########

setnames(contract_grp_sub, "acct_nbr", "account_number")
contract_grp_sub_lifts <- contract_grp_sub[,list(infopro_div_nbr,account_number,lifts_per_week,revenue_annual, revenue_per_bill)]

contract_grp_sub_date <- contract_grp_sub[,list(infopro_div_nbr,account_number,container_cd,close_date)]

sumByCols <- c("infopro_div_nbr","account_number")

contract_grp_sub_lifts <- contract_grp_sub_lifts[,lapply(.SD,sum),by=sumByCols,.SDcols=c("lifts_per_week","revenue_annual", "revenue_per_bill")]

print_var <- paste0(print_var,"\n","18. Collapsing the sites and summing revenue to account level (COntract_grp_sub_lifts)  ",nrow(contract_grp_sub_lifts))

contract_grp_sub_lifts[,rev_per_lift:=revenue_annual/(lifts_per_week*52)]

setnames(contract_grp_sub_lifts, "revenue_annual", "base_revenue")
setnames(contract_grp_sub_lifts, "rev_per_lift", "base_rev_per_lift")

#contract_grp_sub_lifts[,`:=`(revenue=NULL)]

#setting RCF and max(RCF) if more than one site and different RCF for it


nrow(resi_cust_info_sub )

##merge witih resi email data 
nrow(resi_cust_info_sub)
resi_email<-unique(resi_cust_info_sub[,list(infopro_div_nbr, account_number, email)])
nrow(resi_email)

#checking for dups on email address
resi_email$dups<-1
resi_email[,dups:=sum(dups),by=list(infopro_div_nbr, account_number)]
nrow(resi_email[dups>1])
#done checking

setkeyv(resi_email, c("infopro_div_nbr", "account_number"))
setkeyv(contract_grp_sub_lifts, c("infopro_div_nbr", "account_number"))
nrow(contract_grp_sub_lifts)
contract_grp_sub_lifts<-merge(contract_grp_sub_lifts, resi_email)
nrow(contract_grp_sub_lifts)

print_var <- paste0(print_var,"\n","19. Matching the number of records in revenue table with the one in table with email and other customer information  ",nrow(contract_grp_sub_lifts))


#contract_grp_sub <- contract_grp_sub[paste0(infopro_div_nbr,account_number,site_nbr,cont_grp) %in% 
#                                       paste0(resi_cust_info_sub$infopro_div_nbr,
#                                              resi_cust_info_sub$account_number,resi_cust_info_sub$site_nbr,
#                                              resi_cust_info_sub$cont_grp)]
#
##YW Exclusion
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
setkey(resi_cust_info_subset, infopro_div_nbr, account_number, area, business_unit,lawson_company, email)

setkey(resi_cust_info_sub_email, infopro_div_nbr, account_number, area, business_unit,lawson_company, email)

nrow(resi_cust_info_sub_email)

nrow(resi_cust_info_subset)
resi_cust_info_subset <- resi_cust_info_sub_email[resi_cust_info_subset]
resi_cust_info_subset [,`:=`(i.account_open_date=NULL)]


# resi_cust_info_subset1 <- copy(resi_cust_info)[, list(resi_cust_info_subset]


nrow(resi_cust_info_subset)

nrow(contract_grp_sub)
contract_grp_sub <- unique(contract_grp_sub[, list(infopro_div_nbr, account_number, RCF)], by = c("infopro_div_nbr", "account_number"))
nrow(contract_grp_sub)
setkey(contract_grp_subl,infopro_div_nbr,account_number)
setkey(resi_cust_info_subset,infopro_div_nbr,account_number)

print_var <- paste0(print_var,"\n","20. records in revenue table before merger -",nrow(contract_grp_sub))
print_var <- paste0(print_var,"\n","21. records in email table before merger -",nrow(resi_cust_info_subset))


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
print_var <- paste0(print_var,"\n","21. records in table after merger -",nrow(resi_cust_info_sub2))
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
                                                  account_number=paste0(ifcomp,ifacct ),
                                                  NRD=as.numeric(paste0(substr(next_review_dt,1,4),
                                                                        substr(next_review_dt,6,7),
                                                                        substr(next_review_dt,9,10))))])

setkey(next_review_dt_sub,infopro_div_nbr,account_number)
#View(next_review_dt_sub[duplicated(next_review_dt_sub)])
nrow(resi_cust_info_sub2)
resi_cust_info_sub3 <- merge(resi_cust_info_sub2,next_review_dt_sub)
nrow(resi_cust_info_sub3)
resi_cust_info_sub3[,account_open_date:=as.numeric(gsub("-","",account_open_date))]

resi_cust_info_sub3[,`:=`(tenure_years=as.numeric(Sys.Date() - as.Date(as.character(account_open_date),"%Y%m%d"))/365,
                          tenure_months=as.numeric(Sys.Date() - as.Date(as.character(account_open_date),"%Y%m%d"))/(365/12))]

setnames(resi_cust_info_sub3, "revenue_per_bill", "base_revenue_per_bill")

#######################  Start: Adding Fee flags for accounts and Fee charges for divisions  #######################

fee_flags <- data.table(sqlProc(server="prodbirpt", db="DWCORE",
                                sqlFile="SELECT Infopro_Div_Nbr,Fee_Type,is_Fee_Charged,Acct_Nbr
                                FROM Dim_Acct_Fee where CURRENT_TIMESTAMP between Eff_Dt and Disc_Dt and
                                Fee_Type in ('FRF','EVR','INV') and Infopro_Div_Nbr in ('894','899','923','858','282','320','865')",
                                asText=TRUE,windows_auth = TRUE, as.is = TRUE))

fee_flag_rates <- data.table(sqlProc(server="prodbmisql01", db="BMIDM",
                                     sqlFile = "SELECT Infopro_Div_Nbr, frf_rate, erf_rate, admin_fee, erf_on_frf
                                     FROM dbo.divisionFeeRate where Infopro_Div_Nbr in ('894','899','923','858','282','320','865')",
                                     asText = TRUE, windows_auth = TRUE, as.is= TRUE))

fee_flags <- fee_flags[infopro_div_nbr %in% as.character(as.numeric(contract_grp$infopro_div_nbr))]

fee_flags[,`:=`(infopro_div_nbr=sprintf("%03.f", as.numeric(infopro_div_nbr)),
                account_number=paste0(sprintf("%03.f", as.numeric(infopro_div_nbr)),
                                      sprintf("%07.f", as.numeric(acct_nbr))) )]



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

resi_cust_info_sub3[, `:=` (email.y = NULL)]
setnames(resi_cust_info_sub3, "email.x", "Email")


#Calculating ERF FRF and Admin Fees and adding to Revenue, creating column total revenue.
# resi_cust_info_sub3[,`:=`(admin_fee = as.numeric(admin_fee)/RCF)]
# resi_cust_info_sub3[,`:=`(Base_Charge= (base_revenue+as.numeric(admin_fee)*(as.numeric(INV))))]
#Calculating ERF FRF and Admin Fees and adding to Revenue, creating column total revenue.


resi_cust_info_sub3[,`:=`(revenue_with_admin= (as.numeric(base_revenue)+as.numeric(admin_fee)*(as.numeric(INV))))]

resi_cust_info_sub3[,`:=`(admin_fee = as.numeric(admin_fee)/RCF)]

resi_cust_info_sub3[,`:=`(revenue_per_bill_with_admin= (as.numeric(base_revenue_per_bill)+as.numeric(admin_fee)*(as.numeric(INV))))]

resi_cust_info_sub3[,`:=`(FRF_Charge= ((as.numeric(FRF))*revenue_with_admin*(as.numeric(frf_rate)/100)))]
resi_cust_info_sub3[,`:=`(ERF_Charge= ((as.numeric(EVR)*as.numeric(erf_rate)/100)*(revenue_with_admin + as.numeric(erf_on_frf) * FRF_Charge)))]

resi_cust_info_sub3[,`:=`(FRF_Charge_per_bill= ((as.numeric(FRF))*revenue_per_bill_with_admin*(as.numeric(frf_rate)/100)))]
resi_cust_info_sub3[,`:=`(ERF_Charge_per_bill= ((as.numeric(EVR)*as.numeric(erf_rate)/100)*(revenue_per_bill_with_admin + as.numeric(erf_on_frf) * FRF_Charge)))]


resi_cust_info_sub3[,`:=`(revenue = revenue_with_admin + FRF_Charge + ERF_Charge )]

resi_cust_info_sub3[,`:=`(revenue_per_bill = revenue_per_bill_with_admin + FRF_Charge_per_bill + ERF_Charge_per_bill)]

resi_cust_info_sub3[,rev_per_lift:=revenue/(lifts_per_week*52)]



resi_cust_info_sub3[, `:=`(frf_rate= NULL, erf_rate= NULL, admin_fee=NULL, erf_on_frf=NULL)]


# setcolorder(resi_cust_info_sub3,c("infopro_div_nbr","account_number","area","business_unit","lawson_company",
#                                   "email","account_open_date","base_revenue" , "base_rev_per_lift","lifts_per_week","waste_category",
#                                   "close_date","NRD" ,"tenure_years","tenure_months", "FRF_Charge", "ERF_Charge", "revenue", "rev_per_lift"))



lessthan18months <- nrow(copy(resi_cust_info_sub3)[tenure_months <= 18])
lessthan18months
nrow(resi_cust_info_sub3)
resi_cust_info_sub4 <- copy(resi_cust_info_sub3)[tenure_months > 18]

print_var <- paste0(print_var,"\n","22.Records after removing tenure less than 18 months",nrow(resi_cust_info_sub4))

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

setnames(ServiceAddr, "city", "Service_City")
setnames(ServiceAddr, "state", "Service_State")
setnames(ServiceAddr, "acct_nbr", "account_number")

ServiceAddr[, `:=`(account_number = paste0(substr(account_number, 1,3), "-", substr(account_number, 4,10)))]
percentile_test[, `:=`(account_number = paste0(substr(account_number, 1,3), "-", substr(account_number, 4,10)))]
setkey(percentile_test, account_number)
setkey(ServiceAddr, account_number)
percentile_test <- ServiceAddr[percentile_test]


setkey(percentile_test,profitability)

rm(list=setdiff(ls(), c("percentile_test","resi_cust_info_sub3","resi_cust_info_sub2","resi_cust_info_sub", "print_var", "ServiceAddr")))


#View(percentile_test)
nrow(percentile_test)

percentile_test_rev_invalid <- percentile_test[is.na(revenue_per_bill) | revenue_per_bill <=0]

percentile_test <- percentile_test[!is.na(revenue_per_bill) & revenue_per_bill > 0]

percentile_test <- percentile_test[!is.na(profitability) & profitability > 0]

print_var <- paste0(print_var,"\n","24. Records after removing zero or negative profitability customers",nrow(percentile_test))

nrow(percentile_test)
percentile_test[,global_rank:=round(as.numeric(gsub("%","",names(quantile(profitability, probs = seq(0, 1, by= 1/(nrow(percentile_test)-1)))))),2)]

##Stratification

percentile_test[,cat_tenure:=ifelse(tenure_years <=3,"less than 3 yrs",
                                    ifelse(tenure_years < 6,"3 to 6 years","greater than 6 yrs"))]

setkey(percentile_test,infopro_div_nbr,cat_tenure)

percentile_test[,local_rank:=round(as.numeric(gsub("%","",names(quantile(profitability, probs = seq(0, 1, by= 1/(.N-1)))))),2),by=list(infopro_div_nbr,cat_tenure)]

percentile_test[,`:=`(cat_rank=ifelse(local_rank <= 25,"0-25%",ifelse(local_rank <= 75,"25-75%","75-100%")))]
nrow(percentile_test)
percentile_test <- percentile_test[!(gsub(" ","",waste_category) %in% c("SW/YW","YW","RCY/YW","SW/RCY/YW"))]

print_var <- paste0(print_var,"\n","25.Records after removing customers who have Yard Waste - ",nrow(percentile_test))

nrow(percentile_test)

percentile_test[,.N,by=list(infopro_div_nbr, cat_tenure, cat_rank, waste_category)]

percentile_test[,grp:=runif(.N,0,1),by=list(infopro_div_nbr, cat_tenure, cat_rank, waste_category)]

percentile_test[,grp2:=ifelse(grp >= 0.8,"HO",
                              ifelse((grp >= 0.55 & grp != "HO"),"A11",
                                                    ifelse(grp >= 0.3 & grp !="A11" & grp !="HO", "A12",
                                                           ifelse(grp >=0.15 & grp!="A11" & grp!= "HO"& grp!= "A12", "A21",
                                                                  "A22"
                                                           ))))]

################ Current Revenue is Year_1 rate & then calculating Year_2 and Year_3 rates ##############

percentile_test[, `:=` (Year_1 = ifelse(grp2 == "HO", 0 , (base_revenue/12)))]
percentile_test[, `:=`(Year_2 = ifelse(grp2 == "A11" | grp2 == "A12", round(as.numeric(Year_1) + as.numeric(Year_1)*0.04, digits = 2),
                                ifelse(grp2 == "A21" | grp2 == "A22", round(as.numeric(Year_1) + as.numeric(Year_1)*0.1, digits = 2), NA_character_)))] 
percentile_test[, `:=`(Year_3 = ifelse(grp2 == "A11" | grp2 == "A12", round(as.numeric(Year_2) + as.numeric(Year_2)*0.04, digits = 2),
                                ifelse(grp2 == "A21" | grp2 == "A22", round(as.numeric(Year_2) + as.numeric(Year_2)*0.1, digits = 2), NA_character_)))] 
# percentile_test[,list(HO=sum(ifelse(grp2=="HO",1.0,0.0))/.N,
#                A1=sum(ifelse(grp2=="A1",1.0,0.0))/.N,
#                A2=sum(ifelse(grp2=="A2",1.0,0.0))/.N),by=list(infopro_div_nbr, cat_tenure, cat_rank, waste_category)]


nrow(percentile_test)

# percentile_test[, flag:=1]

percentile_test <- percentile_test[, `:=` (SW = ifelse(waste_category == "SW" | waste_category == "SW/RCY", 1, ""))]
percentile_test <- percentile_test[, `:=` (RCY = ifelse(waste_category == "RCY" | waste_category == "SW/RCY", 1, ""))]
percentile_test[, waste_category := NULL]


# percentile_test[,Service_Zip := str_extract(service_address_line_2,"[[:digit:]]{5}$")]
# percentile_test[,Service_State := str_extract(service_address_line_2,"\\s[[:alnum:]]{2}\\s")]
# percentile_test[,Service_City:= gsub("\\s[[:alnum:]]{2}\\s[[:digit:]]{5}$","", service_address_line_2)]

# percentile_test[, service_address_line_2 := NULL]

write.table(percentile_test,paste0("//prodpricapp02/d/Analytics/Resi Email Retention/email_ret_data1.csv"),
            sep=",",na="",row.names=FALSE)

marketingdata <- copy(percentile_test)

marketingdata <- marketingdata[, list( infopro_div_nbr ,  account_number, customer_service_number, doing_business_as ,  Email ,  customer_name ,  acct_address_line_1 ,  
                                        city ,  state ,  postal_code ,  telephone_number_1 ,RCY ,  SW ,  
                                         Service_Address,  Service_City, Service_State, Service_Zip , 
                                        INV, EVR, FRF, grp2,  Year_1, Year_2, Year_3 )]

# setcolorder(marketingdata2 ,c("infopro_div_nbr", "account_nbr", "email", "customer_name", "acct_address_line_1", "acct_address_line_2", "city",
#             "state", "postal_code", "telephone_number_1", "service_address_name", "Service_Zip", "Service_State", "Service_City", "RCY", "SW", "SW"/RCY, "revenue"))

write.table(marketingdata, paste0("//prodpricapp02/d/Analytics/Resi Email Retention/email_ret_data_mark.csv"),
            sep=",",na="",row.names=FALSE)


# save.image("//prodpricapp02/d/Analytics/Resi Email Retention/Image.Rdata")
