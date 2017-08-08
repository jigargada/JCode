#library("timeDate", lib.loc="~/R/win-library/3.2")
#library("xlsx", lib.loc="~/R/win-library/3.2")
#library("xlsxjars", lib.loc="~/R/win-library/3.2")
#library(rsgutilr)
#library("data.table", lib.loc="C:/R/library")
#library("lubridate", lib.loc="C:/R/library")

activity <- "prep" #prep or upload
activity_com <- "prep" #prep or non-prep
#atlanta_800_file <- "Atlanta Template_Sept 2017 Final" # Please rename to the filename returned
apt_analysis <- FALSE

running_month <- Sys.Date()
if(activity == "upload"){
  running_month <- running_month %m-% months(1)
}
pi_effec_date <- as.POSIXlt(running_month %m+% months(4))
pi_effec_date_com <- as.POSIXlt(running_month %m+% months(2))
#pi_effec_date$mon <- pi_effec_date$mon - 1
pi_effec_date$mday <- 1
pi_effec_date_com$mday <- 1
pi_effec_date <- as.character(pi_effec_date)
pi_effec_date_com <- as.character(pi_effec_date_com)
nrd_date_filter <- substr(gsub("-","",as.character(running_month)),1,6)
output_files <- paste0(substr(nrd_date_filter,5,7),"-",substr(nrd_date_filter,1,4),
                       " - File Month - NRD ",substr(nrd_date_filter,5,7),
                       "-",substr(nrd_date_filter,1,4))
period <- paste0(substr(nrd_date_filter,5,7),"_",substr(nrd_date_filter,1,4))
eff_dt_filter <- as.Date(paste0(nrd_date_filter,"01"), "%Y%m%d")
eff_dt_filter_com_format <- as.Date(paste0(nrd_date_filter,"01"), "%Y%m%d")
month(eff_dt_filter) <- month(eff_dt_filter) + 4
month(eff_dt_filter_com_format) <- month(eff_dt_filter_com_format) + 2
eff_dt_filter <- gsub("-","",as.character(eff_dt_filter))
eff_dt_filter_com_format <- gsub("-","",as.character(eff_dt_filter_com_format))
returned_files <- paste0(nrd_date_filter," Final ",eff_dt_filter," Effective Date")

#if(activity == "prep"){
  if (file.exists(paste("D:/Share/YMP Resi/Area Files", output_files,sep="/"))){
    cat("folder exists")
  } else {
    dir.create(file.path("D:/Share/YMP Resi/Area Files", output_files), showWarnings = FALSE)
    filestocopy <- list.files("D:/Share/YMP Resi/Area Files/Folder Structure")
    lapply(filestocopy, function(x) file.copy(paste ("D:/Share/YMP Resi/Area Files/Folder Structure", x , sep = "/"),  
                                              paste ("D:/Share/YMP Resi/Area Files",output_files, sep = "/"),
                                              recursive = TRUE,  copy.mode = TRUE))
  }
#}else{
  if (file.exists(paste("D:/Share/YMP Resi/Area Files/Final", returned_files,sep="/"))){
    cat("folder exists")
  } else {
    dir.create(file.path("D:/Share/YMP Resi/Area Files/Final", returned_files), showWarnings = FALSE)
  }
#}

if(activity_com=="prep"){

  resi_com_format <- data.table(sqlProc(server="prodpricsql02", db="Pricing_Operations",
                                        sqlFile=paste0("SELECT [Group] as rr_brk,[Recommended Rate] as rec_rate,
                                                       left(Area,3) as area,
                                                       SUBSTRING(Area,5,20) as area_nm,
                                                       BU,[Lawson #] as lawson_div,
                                                       [Revenue Distribution Code] as rev_dist_cd,
                                                       Div as infopro,[Customer Account Number] as acct,
                                                       Site as site,[Site Name] as site_name,
                                                       [Site Address] as Addr_Line_1,'' as Addr_Line_2,
                                                       [Site City] as site_city,[Site State] as site_state,
                                                       [Site Zip Code] as site_zip,[Sales Rep] as sales_rep,
                                                       [CONTAINER GROUP] as [group],[Account Type] as acct_type,
                                                       Type as cont_type,QTY as cont_qty,SIZE as size,
                                                       [Lifts Per Month]/4.33 as lifts,[CHARGE CODE] as charge_cd,
                                                       [CHARGE TYPE] as charge_type,[CHARGE METHOD] as charge_method,
                                                       [CURRENT RATE] as curr_charge_rate,([CURRENT RATE]*RCF)/12 as monthly_charge,
                                                       SUBSTRING([Effective Date],7,10)+left([Effective Date],2)+SUBSTRING([Effective Date],4,2) as eff_date,
                                                       RCF,convert(varchar(20), GETDATE(), 101) as NRD,GETDATE() as NRD_2,
                                                       'cfr' as stop_cd,'cfr' as rate_code,[Broker Code] as resi_dist_cd,
                                                       [Spec Handling 1] as sp_handling_1,[Spec Handling 2] as sp_handling_2,
                                                       [Route Note 1] as route_note_1,[Route Note 2] as route_note_2,
                                                       case when [Rate Restr Oper date] is NULL then '' else 
                                                       convert(varchar(20), [Rate Restr Oper date], 101) end as oper_rest_date,
                                                       case when [Disposal Rate Restriction] is NULL then '' else 
                                                       convert(varchar(20), [Disposal Rate Restriction], 101) end as disp_rest_date,
                                                       [FRF flag] as FRF,[Erf flag] as ERF
                                                       FROM [YMPForm_In_",nrd_date_filter,"]
                                                       where [Biz ID] = 'RES' and LEFT(model,1) != 'O'"),
                                          asText=TRUE,windows_auth = TRUE, as.is = TRUE))
  
  resi_com_format <- resi_com_format[!(infopro == 803 & sales_rep %in% c("CPARKER803","MGRANT803","HOA803"))]
  
  resi_com_format[,`:=`(infopro=sprintf("%03d", as.integer(trimws(infopro,which=c("both")))),
                        acct=trimws(acct,which=c("both")),
                        site=trimws(site,which=c("both")),
                        group=trimws(as.character(group),which=c("both")),
                        rec_rate=as.character(round(as.numeric(rec_rate),2)))]
  
  resi_com_format_rr_brk <- resi_com_format[rr_brk %in% c("RTR","BRK")]
  
  upload_file_com_rr_brk <- resi_com_format_rr_brk[.N>0,list(Index=seq(1:.N),
                                                             Comp=sprintf("%03.f", as.numeric(infopro)),
                                                             #Acct=sprintf("%07.f", as.numeric(Acct_nbr)),
                                                             Acct=as.numeric(acct),
                                                             #Site=as.character(Site_nbr),
                                                             Site=sprintf("%05.f", as.numeric(site)),
                                                             CGrp=as.numeric(group),
                                                             Type=as.character(cont_type),
                                                             QTY=as.numeric(cont_qty),
                                                             Size=round(as.numeric(size),2),
                                                             Lifts="",comp="",
                                                             'Charge Code'=as.character(charge_cd),
                                                             Type=as.character(charge_type),
                                                             Method=as.character(charge_method),
                                                             'Charge Rate'=as.numeric(curr_charge_rate),
                                                             'New Rate'=round(as.numeric(rec_rate),2),
                                                             'Effective Date'=format(strptime(as.character(eff_dt_filter_com_format), "%Y%m%d"),"%m/%d/%Y"),
                                                             'Trans Code'="3",
                                                             'Reason Code'="64",
                                                             'NA'="N",
                                                             UOM="",
                                                             WMT="")]
  
  write.table(upload_file_com_rr_brk, 
              file = paste0("D:/Share/YMP Resi/Area Files/",output_files,"/upload_file_com_rr_brk.csv"), 
              sep = ",",na = "",row.names=FALSE)
  
  write.table(resi_com_format_rr_brk, 
              file = paste0("D:/Share/YMP Resi/Area Files/",output_files,"/all_data_com_format_rr_brk.csv"), 
              sep = ",",na = "",row.names=FALSE)
  
  resi_com_format <- resi_com_format[!(rr_brk %in% c("RTR","BRK"))]
  resi_com_format[,`:=`(rr_brk=NULL,rec_rate=NULL)]
  
  additional_cols <- data.table(sqlProc(server="prodbirpt", db="DWCORE",
                                        sqlFile=paste0("SELECT DISTINCT cg.infopro_div_nbr as infopro
                                                       ,cg.acct_nbr as acct
                                                       ,cg.site_nbr as site
                                                       ,cg.container_grp_nbr as [group]
                                                       ,convert(varchar(20), cg.Orig_Start_Dt, 101) as orig_start_date
                                                       ,h.[Cur_Region_Nbr] as region
                                                       ,h.[Cur_Region_Nm] as region_nm
                                                       ,h.[Cur_Infopro_Div_Nm] as infopro_nm
                                                       ,ds.[Territory] as sales_territory
                                                       ,ds.[LATITUDE] as lat
                                                       ,ds.[LONGITUDE] as long
                                                       ,daf.[Admin Flag] as admin
                                                       ,cg.contract_grp_nbr as contract_grp_nbr
                                                       ,cg.contract_nbr as contract_nbr
                                                       FROM [DWPSA].[dbo].[STG_IFP_BIPCC_2] bip
                                                       INNER JOIN dwcore.dbo.dim_container_grp cg 
                                                       ON cg.infopro_div_nbr = bip.[CCCOMP] 
                                                       AND cg.acct_nbr = bip.[CCACCT] 
                                                       AND cg.site_nbr = bip.[CCSITE] 
                                                       AND cg.container_grp_nbr = bip.[CCCTGR] 
                                                       AND CURRENT_TIMESTAMP between cg.eff_dt and cg.disc_dt
                                                       AND cg.is_Active = 1
                                                       INNER JOIN dwcore.dbo.dim_corp_hier h 
                                                       ON cg.infopro_div_nbr = h.infopro_div_nbr 
                                                       AND cg.revenue_distribution_cd = h.rev_distrib_cd 
                                                       AND CURRENT_TIMESTAMP between h.eff_dt and h.disc_dt
                                                       AND h.[Cur_LOB_Category] in ('Residential','Residential Recycling')
                                                       LEFT JOIN [DWCORE].[dbo].[Dim_Site] ds
                                                       ON cg.infopro_div_nbr = ds.[Infopro_Div_Nbr] 
                                                       AND cg.acct_nbr = ds.[Acct_Nbr] 
                                                       AND cg.site_nbr = ds.[Site_Nbr]
                                                       AND CURRENT_TIMESTAMP between ds.Eff_Dt and ds.Disc_Dt
                                                       AND ds.is_Split = 0 AND ds.is_Merged = 0
                                                       LEFT JOIN (SELECT [Infopro_Div_Nbr]
                                                       ,max(case when Fee_Type = 'INV' and is_Fee_Charged = 1 then 'Y' else 'N' end) as [Admin Flag]
                                                       ,[Acct_Nbr]
                                                       FROM [DWCORE].[dbo].[Dim_Acct_Fee]
                                                       where CURRENT_TIMESTAMP between Eff_Dt and Disc_Dt and is_Split = 0 and is_Merged = 0
                                                       and Fee_Type in ('INV')
                                                       group by Infopro_Div_Nbr, Acct_Nbr) daf
                                                       ON cg.infopro_div_nbr = daf.Infopro_Div_Nbr
                                                       AND cg.acct_nbr = daf.Acct_Nbr
                                                       where bip.CURRENT_IND = 'Y' and bip.SPLIT_IND = 'N' and bip.CCBADJ = '0'
                                                       and bip.MERGE_IND = 'N' and bip.CCEFDT < cast(format(GETDATE(),'yyyyMMdd') as int)
                                                       and bip.CCCHRT > 0"),
                                      asText=TRUE,windows_auth = TRUE, as.is = TRUE))
  
  additional_cols[,`:=`(infopro=trimws(infopro,which=c("both")),
                        acct=trimws(acct,which=c("both")),
                        site=trimws(site,which=c("both")),
                        group=trimws(group,which=c("both")))]

  setkey(resi_com_format,infopro,acct,site,group)
  setkey(additional_cols,infopro,acct,site,group)
  
  resi_com_format <- additional_cols[resi_com_format]
  resi_com_format_dropped <- resi_com_format[is.na(region)]
  resi_com_format <- resi_com_format[!is.na(region)]
  resi_com_format[,rate_code:=ifelse(contract_grp_nbr==0 & contract_nbr=="",
                                     rate_code,
                                     paste0(contract_grp_nbr,"-",contract_nbr))]
  resi_com_format[,`:=`(contract_grp_nbr=NULL,contract_nbr=NULL,acct_aquisition=NA_character_)]
  rm(additional_cols)
  resi_OM_cols <- tolower(sqlGetColumns(server="prodpricsql02", db="Pricing_Operations", tbl="resi_OM", windows_auth = TRUE))
  
  setcolorder(resi_com_format,resi_OM_cols)
  
  resi_com_format[,nrd:= paste0(substr(nrd_date_filter,5,7),"/",substr(nrd_date_filter,5,7),"/",substr(nrd_date_filter,1,4))]
  
  resi_com_format[,`:=`(infopro=sprintf("%03d", as.integer(infopro)),
                        lifts=as.integer(round(lifts,0)),
                        nrd_2=paste(as.Date(nrd,format="%m/%d/%Y"),"00:00:00.000"))]
  
  sqlInsert(server="prodpricsql02",db="Pricing_Operations",tbl="resi_OM",rdata=resi_com_format,windows_auth=TRUE)
}

if(activity=="prep"){
  infopro_file <- data.table(sqlProc(server="prodpricsql02", db="Pricing_Operations",
                              sqlFile=paste0("Select DISTINCT * from resi_OM WHERE infopro not in ('971','384','972') 
                              and [charge_cd] not in ('RCC','NCH','EVR','NRS','ADM') 
                              and [charge_cd] not like ('FR%') 
                              and [curr_charge_rate] > 0
                              and [curr_charge_rate] <> 1.68
                              AND (([stop_cd] <> 'cfr' AND [eff_date] < '",eff_dt_filter,"') OR
                              ([stop_cd] = 'cfr' AND [eff_date] < '",eff_dt_filter_com_format,"'))
                              AND [eff_date] < '",substr(nrd_date_filter,1,4),"0101'
                              and NRD like ('",substr(nrd_date_filter,5,7),"/__/",substr(nrd_date_filter,1,4),"')"),
                              asText=TRUE,windows_auth = TRUE, as.is = TRUE))
  
  infopro_file[,acct_aquisition:=NULL]
  
  infopro_file <- infopro_file[!(bu=="BU250" & lawson_div==3757 & rev_dist_cd %in% c("31","7A") & round(monthly_charge,2) == 12.44)]
  
  setnames(infopro_file,colnames(infopro_file),
           c("cur_region_nbr","cur_region_nm","cur_area_nbr","cur_area_nm","business_unit","lawson_number",
             "revenue_dist_code","company_number","cur_infopro_div_nm","account_number","site","site_name",
             "addr_line_1","addr_line_2","site_city","site_state","site_zip","lat","long","sales_territory",
             "sales_rep","cont_grp","account_type","container_type","container_qty","container_size",
             "service_freq","charge_code","charge_type","charge_method","charge_rate","monthly_charge","effective_date",
             "orig_start_date","recurring_charge_freq","next_review_date","NRD_formatted","stop_code","rate_type",
             "resi_district","special_handling_1","special_handling_2","route_note_1","route_note_2","rate_restrict_dt",
             "disposal_rate_restrict_dt","frf","erf","admin"))
  
  infopro_file[,`:=`(company_number=gsub(" ","",company_number),
                     account_number=gsub(" ","",account_number),
                     charge_rate=as.numeric(gsub(" ","",charge_rate)),
                     lat=NULL,long=NULL,monthly_charge=NULL,NRD_formatted=NULL)]
  
  #exc_list <- as.data.table(read.table(file = paste0("./data_in/YMP Resi/Area Files/",output_files,"/exclusion_list.csv"),
  #                                     header = TRUE))
  
  #exc_list[,acct_num:=as.character(acct_num)]
  
  #infopro_file <- infopro_file[!(paste0(lawson_number,account_number) %in% exc_list$acct_num)]
  
  codes <- data.table(sqlProc(server="prodbirpt", db="DWPSA",
                              sqlFile="Select tbtbln,tbelem,tbdesc from [DWPSA].[dbo].[STG_IFP_BIPTB] where current_ind = 'Y' and tbstat = 'A' and tbtbln in ('STT','RAT','BIL') order by TBTBLN",
                              asText=TRUE,windows_auth = TRUE, as.is = TRUE))
  
  stop_codes <- codes[tbtbln=="STT"][,tbtbln:=NULL]
  rate_types <- codes[tbtbln=="RAT"][,tbtbln:=NULL]
  charge_codes <- codes[tbtbln=="BIL"][,tbtbln:=NULL]
  charge_codes <- data.table(unique(charge_codes))
  
  setnames(stop_codes,c("tbelem","tbdesc"),c("stop_code","stop_code_desc"))
  setnames(rate_types,c("tbelem","tbdesc"),c("rate_type","rate_type_desc"))
  setnames(charge_codes,c("tbelem","tbdesc"),c("charge_code","charge_code_desc"))
  stop_codes[,`:=`(stop_code=gsub(" ","",stop_code),stop_code_desc=gsub(" ","",stop_code_desc))]
  stop_codes <- rbind(stop_codes,list("cfr","com format"))
  rate_types[,`:=`(rate_type=gsub(" ","",rate_type),rate_type_desc=gsub(" ","",rate_type_desc))]
  rate_types <- rbind(rate_types,list("cfr","com format"))
  charge_codes[,`:=`(charge_code=gsub(" ","",charge_code),charge_code_desc=gsub(" ","",charge_code_desc))]
  setkey(stop_codes,stop_code)
  setkey(rate_types,rate_type)
  setkey(charge_codes,charge_code)
  
  setkey(infopro_file,stop_code)
  infopro_file <- stop_codes[infopro_file]
  setkey(infopro_file,rate_type)
  infopro_file <- rate_types[infopro_file]
  setkey(infopro_file,charge_code)
  charge_codes <- data.table(unique(charge_codes,by="charge_code"))
  infopro_file <- charge_codes[infopro_file]
  
  View(infopro_file[is.na(stop_code_desc) | is.na(rate_type_desc) | is.na(charge_code_desc)])
  
  dist_codes <- data.table(sqlProc(server="prodbirpt", db="DWPSA",
                              sqlFile="Select DISTINCT tbdivn,tbelem,tbdesc from [DWPSA].[dbo].[STG_IFP_BIPTB] 
                              where current_ind = 'Y' 
                              and tbstat = 'A'
                              and tbdivn <> '' 
                              and tbtbln = 'DIS'",
                              asText=TRUE,windows_auth = TRUE, as.is = TRUE))
  
  setnames(dist_codes,c("tbdivn","tbelem","tbdesc"),c("company_number","resi_district","resi_district_desc"))
  dist_codes[,`:=`(company_number=as.character(as.numeric(gsub(" ","",company_number))),
                   resi_district=gsub(" ","",resi_district),
                   resi_district_desc=gsub(" ","",resi_district_desc))]
  
  setkey(dist_codes,company_number,resi_district)
  dist_codes <- data.table(unique(dist_codes,by=c("company_number","resi_district")))
  
  setkey(infopro_file,company_number,resi_district)
  infopro_file <- dist_codes[infopro_file]
  
  View(infopro_file[is.na(resi_district_desc)])
  
  #future_rates <- data.table(sqlProc(server="prodbirpt", db="DWPSA",
  #                                 sqlFile="SELECT [COMPANY_NUMBER],[ACCOUNT_NUMBER],[SITE],[CONTAINER_GROUP] as cont_grp
  #                                 FROM [DWPSA].[dbo].[STG_IFP_BIPCCR]
  #                                 where CLOSE_DATE > 20160914
  #                                 and CURRENT_IND = 'Y'",
  #                                 asText=TRUE,windows_auth = TRUE, as.is = TRUE))
  
  #future_rates[,`:=`(company_number=as.numeric(company_number),
  #                   account_number=as.numeric(account_number),
  #                   site=as.numeric(site),
  #                   cont_grp=as.numeric(cont_grp))]
  
  #future_rates[,acct_key:=paste0(sprintf("%03.0f",company_number),
  #                               sprintf("%07.0f",account_number),
  #                               sprintf("%05.0f",site),
  #                               cont_grp)]
  
  infopro_file[,acct_key:=paste0(sprintf("%03.0f",as.numeric(company_number)),
                                 sprintf("%07.0f",as.numeric(account_number)),
                                 sprintf("%05.0f",as.numeric(site)),
                                 as.numeric(cont_grp))]
  
  #infopro_file <- infopro_file[!(acct_key %in% future_rates$acct_key)]
  
  infopro_file[,`:=`(site_address=ifelse(addr_line_2 == "",addr_line_1,paste0(addr_line_1,", ",addr_line_2)),
                     addr_line_1=NULL,addr_line_2=NULL,acct_key=NULL)]
  
  setcolorder(infopro_file,c('cur_region_nbr','cur_region_nm','cur_area_nbr','cur_area_nm','business_unit','company_number',
                             'cur_infopro_div_nm','stop_code','stop_code_desc','rate_type','rate_type_desc','resi_district',
                             'resi_district_desc','recurring_charge_freq','charge_code','effective_date','orig_start_date',
                             'charge_rate','next_review_date','account_number','site','cont_grp','charge_code_desc',
                             'charge_type','charge_method','account_type','revenue_dist_code','container_type','container_size',
                             'container_qty','service_freq','site_name','site_address','site_city','site_state','site_zip',
                             'sales_territory','sales_rep','lawson_number','special_handling_1','special_handling_2',
                             'route_note_1','route_note_2','rate_restrict_dt','disposal_rate_restrict_dt','frf','erf','admin'))
  
  #infopro_file[,`:=`(lawson_desc=NULL,business_unit_desc=NULL,area=NULL,area_desc=NULL)]
  #Add the column for year and month
  infopro_file[,`:=`(year_month=paste0(format(running_month,"%Y"),
                                       ifelse(nchar(match(format(running_month,"%b"),month.abb))>9,"","0"),
                                       match(format(running_month,"%b"),month.abb)),
                     company_number=gsub(" ","",company_number))]
  
  #split the division 800
  infopro_file_800 <- infopro_file[company_number=="800"]
  if(nrow(infopro_file_800)>0){
    #write.xlsx(infopro_file_800,paste0("D:/Analytics/Pricing Engine/Dev/data_in/YMP Resi/Area Files/",output_files,"/Atlanta_YMP_Resi_flat_file.xlsx"), sheetName="Sheet1", 
    #          col.names=TRUE, row.names=TRUE, append=FALSE, showNA=TRUE)
    
    write.csv(infopro_file_800, file = paste0("D:/Share/YMP Resi/Area Files/",output_files,"/Atlanta_YMP_Resi_flat_file.csv"),row.names=FALSE)
  }
  
  #date_limit <- as.POSIXlt(Sys.Date())
  #date_limit$mon <- date_limit$mon - 1
  #date_limit$mday <- 1
  #date_limit <- as.Date(date_limit)
  
  #infopro_file <- infopro_file[company_number!="800" & 
  #                               !(charge_code %in% c("NCH","RCC","FRX","FRE")) & 
  #                               rate_type != "W" &
  #                               !is.na(rate_type) &
  #                               charge_rate > 0 & 
  #                               !is.na(charge_rate) &
  #                               ((as.Date(as.character(next_review_date), "%Y%m%d") >= date_limit &
  #                                  as.Date(as.character(next_review_date), "%Y%m%d") <= as.Date(timeLastDayInMonth(date_limit))))]
  
  infopro_file <- infopro_file[company_number!="800" & !is.na(rate_type)]
  
  setkey(infopro_file,cur_area_nbr,company_number)
  
  infopro_file[,`:=`(Region_Desc=cur_region_nm,
                     Area_Desc=cur_area_nm,
                     BU=business_unit,
                     Lawson_Company=lawson_number,
                     Division=company_number,
                     Acct_nbr=account_number,
                     Site_nbr=site,
                     Group=cont_grp,
                     Info_Div_Desc=cur_infopro_div_nm,
                     Stop_Code=stop_code,
                     Description=stop_code_desc,
                     Rate_Type=rate_type,
                     Rate_Description=rate_type_desc,
                     Residential_District=resi_district,
                     Residential_Description=resi_district_desc,
                     Charge_Code=charge_code,
                     RCF=recurring_charge_freq,
                     Monthly_Rate=(charge_rate*recurring_charge_freq)/12,
                     New_Monthly_Rate="",
                     Sugg_Monthly_Rate_Rounded="",
                     Increase_percent="",
                     Last_PI_per_Effective_Date=effective_date,
                     Charge_Rate=charge_rate,
                     active_cust=1,
                     Current_Monthly_Revenue="",
                     Proposed_Monthly_Revenue="",
                     Monthly_Increase_in_Revenue="",
                     Enter_New_Monthly_Rate="",
                     New_Charge_Rate="",
                     PI_Effective_Date=ifelse(stop_code=="cfr",pi_effec_date_com,pi_effec_date),
                     Comments="",
                     Variance_to_Monthly_Rate="",
                     Percent_of_PI="",
                     Current_Revenue_PriortoIncrease="",
                     Monthly_Revenue_After_Increase="",
                     Monthly_Revenue_Increase="",
                     Base_Rev_that_was_NOT_increased="",
                     Base_Rev_that_received_an_increase="",
                     change_in_rate="",
                     NRD=next_review_date,
                     frf_flag=ifelse(frf=="Y","yes","no"),
                     erf_flag=ifelse(erf=="Y","yes","no"),
                     admin_flag=ifelse(admin=="Y","yes","no"),
                     revenue_distribution_cd=revenue_dist_code,
                     Index2=seq.int(nrow(infopro_file)))]
  
  keepCols <- c('Region_Desc','Area_Desc','BU','Lawson_Company','Division','Acct_nbr','Site_nbr','Group',
                'Info_Div_Desc','Stop_Code','Description','Rate_Type','Rate_Description','Residential_District',
                'Residential_Description','Charge_Code','RCF','Monthly_Rate','New_Monthly_Rate','Sugg_Monthly_Rate_Rounded',
                'Increase_percent','Last_PI_per_Effective_Date','Charge_Rate','active_cust',
                'Current_Monthly_Revenue','Proposed_Monthly_Revenue','Monthly_Increase_in_Revenue',
                'Enter_New_Monthly_Rate','New_Charge_Rate','PI_Effective_Date','Comments',
                'Variance_to_Monthly_Rate','Percent_of_PI','Current_Revenue_PriortoIncrease',
                'Monthly_Revenue_After_Increase','Monthly_Revenue_Increase','Base_Rev_that_was_NOT_increased',
                'Base_Rev_that_received_an_increase','change_in_rate','NRD','frf_flag','erf_flag','admin_flag',
                'special_handling_1','special_handling_2','route_note_1','route_note_2','rate_restrict_dt',
                'disposal_rate_restrict_dt','revenue_distribution_cd','Index2','container_type','container_qty',
                'container_size','service_freq','charge_type','charge_method')
  
  infopro_file <- infopro_file[,keepCols,with=FALSE]
  save(infopro_file,file = paste0("D:/Share/YMP Resi/Area Files/",output_files,"/infopro_file.rdata"))
}

if(activity == "upload"){
  load(file=paste0("D:/Share/YMP Resi/Area Files/",output_files,"/infopro_file.rdata"))
}

great_lakes <- infopro_file[Area_Desc == "GREAT LAKES"]
heartland <- infopro_file[Area_Desc == "HEARTLAND"]
mid_atlantic <- infopro_file[Area_Desc == "MID-ATLANTIC"]
midwest <- infopro_file[Area_Desc == "MIDWEST"]
northeast <- infopro_file[Area_Desc == "NORTHEAST"]
northwest <- infopro_file[Area_Desc == "NORTHWEST"]
south <- infopro_file[Area_Desc == "SOUTH"]
southeast <- infopro_file[Area_Desc == "SOUTHEAST"]
southwest <- infopro_file[Area_Desc == "SOUTHWEST"]
west <- infopro_file[Area_Desc == "WEST"]

keepCols <- c('Index','Region_Desc','Area_Desc','BU','Lawson_Company','Division','Acct_nbr','Site_nbr','Group',
              'Info_Div_Desc','Stop_Code','Description','Rate_Type','Rate_Description','Residential_District',
              'Residential_Description','Charge_Code','RCF','Monthly_Rate','New_Monthly_Rate','Sugg_Monthly_Rate_Rounded',
              'Increase_percent','Last_PI_per_Effective_Date','Charge_Rate','active_cust',
              'Current_Monthly_Revenue','Proposed_Monthly_Revenue','Monthly_Increase_in_Revenue',
              'Enter_New_Monthly_Rate','New_Charge_Rate','PI_Effective_Date','Comments',
              'Variance_to_Monthly_Rate','Percent_of_PI','Current_Revenue_PriortoIncrease',
              'Monthly_Revenue_After_Increase','Monthly_Revenue_Increase','Base_Rev_that_was_NOT_increased',
              'Base_Rev_that_received_an_increase','change_in_rate','NRD','frf_flag','erf_flag','admin_flag',
              'special_handling_1','special_handling_2','route_note_1','route_note_2','rate_restrict_dt',
              'disposal_rate_restrict_dt','revenue_distribution_cd','Index2','container_type','container_qty',
              'container_size','service_freq','charge_type','charge_method')

areas <- c()

if(nrow(great_lakes)>0) {areas <- c(areas,"great_lakes")}
if(nrow(heartland)>0) {areas <- c(areas,"heartland")}
if(nrow(mid_atlantic)>0) {areas <- c(areas,"mid_atlantic")}
if(nrow(midwest)>0) {areas <- c(areas,"midwest")}
if(nrow(northeast)>0) {areas <- c(areas,"northeast")}
if(nrow(northwest)>0) {areas <- c(areas,"northwest")}
if(nrow(south)>0) {areas <- c(areas,"south")}
if(nrow(southeast)>0) {areas <- c(areas,"southeast")}
if(nrow(southwest)>0) {areas <- c(areas,"southwest")}
if(nrow(west)>0) {areas <- c(areas,"west")}

#areas <- c("great_lakes","heartland","mid_atlantic","midwest","northeast","northwest","south","southeast","southwest","west")

#rm(BI_corp_hier,fee_flags,notes,RR,codes,infopro_file,stop_codes)
gc()

if(activity == "prep"){
    for(j in 1:length(areas)){
      
      unq_BUs <- unique(eval(parse(text = paste0(areas[j])))$BU)
      
      for(i in 1:length(unq_BUs)){
        DT <- eval(parse(text = paste0(areas[j])))[BU==unq_BUs[i]]
        setkey(DT, Division,Stop_Code,Rate_Type,Residential_District,Charge_Code,RCF,Monthly_Rate,Charge_Rate)
        DT[,Index:=.GRP,by=key(DT)]
        DT <- DT[,keepCols,with=FALSE]
        
        #sumByCols <- c("Index","Region_Desc","Area_Desc","BU","Division","Stop_Code",
        #               "Description","Rate_Type","Rate_Description","Residential_District","Residential_Description",
        #               "Charge_Code","RCF",'Monthly_Rate',"New_Monthly_Rate","Sugg_Monthly_Rate_Rounded",
        #               "Increase_percent","Charge_Rate","Current_Monthly_Revenue","Proposed_Monthly_Revenue",
        #               "Monthly_Increase_in_Revenue","Enter_New_Monthly_Rate","New_Charge_Rate","PI_Effective_Date",
        #               "Comments","Variance_to_Monthly_Rate","Percent_of_PI","Current_Revenue_PriortoIncrease",
        #               "Monthly_Revenue_After_Increase","Monthly_Revenue_Increase","Base_Rev_that_was_NOT_increased",
        #               "Base_Rev_that_received_an_increase","change_in_rate","NRD")
        
        sumByCols <- c("Index","Region_Desc","Area_Desc","BU","Division","Stop_Code",
                       "Description","Rate_Type","Rate_Description","Residential_District","Residential_Description",
                       "Charge_Code","RCF",'Monthly_Rate',"New_Monthly_Rate","Sugg_Monthly_Rate_Rounded",
                       "Increase_percent","Charge_Rate","Current_Monthly_Revenue","Proposed_Monthly_Revenue",
                       "Monthly_Increase_in_Revenue","Enter_New_Monthly_Rate","New_Charge_Rate","PI_Effective_Date",
                       "Comments","Variance_to_Monthly_Rate","Percent_of_PI","Current_Revenue_PriortoIncrease",
                       "Monthly_Revenue_After_Increase","Monthly_Revenue_Increase","Base_Rev_that_was_NOT_increased",
                       "Base_Rev_that_received_an_increase","change_in_rate")
        
        smmry <- DT[,lapply(.SD,sum),by=sumByCols,.SDcols=c("active_cust")]
        
        l <- list(DT,smmry)
        DT <- rbindlist(l,fill=TRUE)
        setkey(DT,Index,active_cust)
        
        DT[,`:=`(New_Monthly_Rate=paste0("=S",as.character(seq.int(nrow(DT))+9),"*(1+","IF(AND(K",as.character(seq.int(nrow(DT))+9),"=Q$8,BC",as.character(seq.int(nrow(DT))+9),">1)",",Q$7,Q$6","))"),
                 Sugg_Monthly_Rate_Rounded=paste0("=IF(T",
                                                  as.character(seq.int(nrow(DT))+9),
                                                  "-S",
                                                  as.character(seq.int(nrow(DT))+9),
                                                  "<0.75,S",
                                                  as.character(seq.int(nrow(DT))+9),
                                                  "+0.75,CEILING(T",
                                                  as.character(seq.int(nrow(DT))+9),
                                                  ",Q$5))"),
                          Increase_percent=paste0("=(U",
                                                  as.character(seq.int(nrow(DT))+9),
                                                  "/S",
                                                  as.character(seq.int(nrow(DT))+9),
                                                  ")-1"),
                          Current_Monthly_Revenue=active_cust * Monthly_Rate,
                          Proposed_Monthly_Revenue=paste0("=Y",
                                                          as.character(seq.int(nrow(DT))+9),
                                                          "*U",
                                                          as.character(seq.int(nrow(DT))+9)),
                          Monthly_Increase_in_Revenue=paste0("=AA",
                                                             as.character(seq.int(nrow(DT))+9),
                                                             "-Z",
                                                             as.character(seq.int(nrow(DT))+9)),
                          New_Charge_Rate=paste0("=(AC",
                                                 as.character(seq.int(nrow(DT))+9),
                                                 "*12/R",
                                                 as.character(seq.int(nrow(DT))+9),
                                                 ")"),
                          Variance_to_Monthly_Rate=paste0("=AC",
                                                          as.character(seq.int(nrow(DT))+9),
                                                          "-S",
                                                          as.character(seq.int(nrow(DT))+9)),
                          Percent_of_PI=paste0("=(AD",
                                               as.character(seq.int(nrow(DT))+9),
                                               "/X",
                                               as.character(seq.int(nrow(DT))+9),
                                               ")-1"),
                          Current_Revenue_PriortoIncrease=active_cust * Monthly_Rate,
                          Monthly_Revenue_After_Increase=paste0("=IF(AC",
                                                                as.character(seq.int(nrow(DT))+9),
                                                                "=0,AI",
                                                                as.character(seq.int(nrow(DT))+9),
                                                                ",((AD",
                                                                as.character(seq.int(nrow(DT))+9),
                                                                "*R",
                                                                as.character(seq.int(nrow(DT))+9),
                                                                ")/12)*Y",
                                                                as.character(seq.int(nrow(DT))+9),
                                                                ")"),
                          Monthly_Revenue_Increase=paste0("=AJ",
                                                          as.character(seq.int(nrow(DT))+9),
                                                          "-AI",
                                                          as.character(seq.int(nrow(DT))+9)),
                          Base_Rev_that_was_NOT_increased=paste0("=IF(AK",
                                                                 as.character(seq.int(nrow(DT))+9),
                                                                 "=0,AI",
                                                                 as.character(seq.int(nrow(DT))+9),
                                                                 ",0)"),
                          Base_Rev_that_received_an_increase=paste0("=IF(AK",
                                                                    as.character(seq.int(nrow(DT))+9),
                                                                    ">0,AI",
                                                                    as.character(seq.int(nrow(DT))+9),
                                                                    ",0)"),
                          change_in_rate=paste0("=IF(AC",
                                                as.character(seq.int(nrow(DT))+9),
                                                "=0,$U$1,AC",
                                                as.character(seq.int(nrow(DT))+9),
                                                "-S",
                                                as.character(seq.int(nrow(DT))+9),
                                                ")"))]
        
        if(areas[j] == "great_lakes"){
          DT[,`:=`(Enter_New_Monthly_Rate=paste0("=(CEILING(IF(Z",as.character(seq.int(nrow(DT))+9),
                                                 "<15,Z",as.character(seq.int(nrow(DT))+9),
                                                 "*1.2,IF(Z",as.character(seq.int(nrow(DT))+9),
                                                 "<24,Z",as.character(seq.int(nrow(DT))+9),
                                                 "*1.15,Z",as.character(seq.int(nrow(DT))+9),
                                                 "*1.1)),0.25))"))]
        }
        
        write.table(DT,paste0("D:/Share/YMP Resi/Area Files/",
                            output_files,"/",areas[j],"/CSV/",areas[j],"_",unq_BUs[i],".csv"),
        sep=",",na="",row.names=FALSE,col.names = FALSE)
        
        rm(DT)
        rm(sumByCols)
        rm(smmry)
        gc()
      }
      #rm(eval(parse(text = paste0(areas[j]))))
      gc()
    }
    shell('powershell -NonInteractive -File "D:/Gaurav/Pricing-Analytics/processes/YMP Resi/template/test_macro.ps1"')
    #write.table(future_rates,paste0("D:/Analytics/Pricing Engine/Dev/data_in/YMP Resi/Area Files/",
    #                                output_files,"/future_rates.csv"),sep=",",na="",row.names=FALSE)
}else{
  #areas <- c("great_lakes","heartland","mid_atlantic","midwest","northeast","northwest","south","southeast","southwest","west")
  library("excel.link", lib.loc="~/R/win-library/3.3")
  count <- 0
  rowimp2 <- 0
  rowimp3 <- 0
  for(j in 1:length(areas)){
    count2 <- 0
    unq_BUs <- unique(eval(parse(text = paste0(areas[j])))$BU)
    #unq_BUs[unq_BUs=="BU123"] <- "BU123_1"
    #if(!is.na(match('BU123_1',unq_BUs))){
    #  unq_BUs <- c(unq_BUs, "BU123_2")
    #}
    for(i in 1:length(unq_BUs)){
      file_name <- list.files(path = paste0("D:/Share/YMP Resi/Area Files/Final/",
                                            returned_files,"/",areas[j],"/"),
                              pattern = paste0("^(.|\n)*",unq_BUs[i],"(.|\n)*\\.xlsb$"),
                              full.names = FALSE)
      
      subDir <- paste0("D:/Share/YMP Resi/Area Files/Final/",returned_files,"/",areas[j],"/",file_name)
      if (file.exists(subDir)){
        count <- count + 1
        count2 <- count2 + 1
        #cat(count2,"_",paste0(areas[j],"_",unq_BUs[i],"\n"))
        test <- data.table(xl.read.file(subDir,header = FALSE, xl.sheet = "Data", top.left.cell = "A10"))
        
        #c(letters, sort(do.call("paste0", expand.grid(letters, letters[1:26]))))[1:58]
        setnames(test,colnames(test)[1:58],
                 c('Index2','Region_Desc','Area_Desc','BU','Lawson_Company','Division','Acct_nbr','Site_nbr','Group',
                   'Info_Div_Desc','Stop_Code','Description','Rate_Type','Rate_Description','Residential_District',
                   'Residential_Description','Charge_Code','RCF','Monthly_Rate','New_Monthly_Rate','Sugg_Monthly_Rate_Rounded',
                   'Increase_percent','Last_PI_per_Effective_Date','Charge_Rate','active_cust',
                   'Current_Monthly_Revenue','Proposed_Monthly_Revenue','Monthly_Increase_in_Revenue',
                   'Enter_New_Monthly_Rate','New_Charge_Rate','PI_Effective_Date','Comments',
                   'Variance_to_Monthly_Rate','Percent_of_PI','Current_Revenue_PriortoIncrease',
                   'Monthly_Revenue_After_Increase','Monthly_Revenue_Increase','Base_Rev_that_was_NOT_increased',
                   'Base_Rev_that_received_an_increase','change_in_rate','NRD','frf_flag','erf_flag','admin_flag',
                   'special_handling_1','special_handling_2','route_note_1','route_note_2','rate_restrict_dt',
                   'disposal_rate_restrict_dt','revenue_distribution_cd','Index','container_type','container_qty',
                   'container_size','service_freq','charge_type','charge_method'))
        
        rowimp <- nrow(test)
        rowimp2 <- rowimp2 + nrow(test)
        test <- test[!is.na(Index)]
        rowimp3 <- rowimp3 + nrow(test)
        cat(count2,"_",paste0(areas[j],"_",unq_BUs[i],"_rows imported - ",rowimp,"_rows kept - ",nrow(test),"_pretotal ",rowimp2,"_posttotal ",rowimp3,"\n"))
        if(class(test$NRD)[1]=="POSIXct"){
          test[,NRD:=as.Date(NRD)]
        }else{
          if(class(test$NRD)=="numeric"){
            test[,NRD:=as.Date(NRD,origin="1899-12-30")]
          }
          if(class(test$NRD)=="character"){
            test[,NRD:=as.Date(ifelse(grepl("-",NRD),as.Date(NRD,format="%Y-%m-%d"),as.Date(as.integer(NRD),origin="1899-12-30")),origin=origin)]
          }
        }
        test[,`:=`(rate_restrict_dt=as.character(rate_restrict_dt),
                   disposal_rate_restrict_dt=as.character(disposal_rate_restrict_dt),
                   PI_Effective_Date=as.Date(PI_Effective_Date))]
        
        test_com <- test[Stop_Code=="cfr"]
        
        test_res <- test[Stop_Code!="cfr"]
        
        cat(paste0(nrow(test_com)+nrow(test_res),"\n"))
        
        upload_file_res <- test_res[as.numeric(Charge_Rate) < as.numeric(New_Charge_Rate),
                            list(Index=as.numeric(Index),
                                 Comp=sprintf("%03.f", as.numeric(Division)),
                                 #Acct=sprintf("%07.f", as.numeric(Acct_nbr)),
                                 Acct=as.numeric(Acct_nbr),
                                 #Site=as.character(Site_nbr),
                                 Site=sprintf("%05.f", as.numeric(Site_nbr)),
                                 CGrp=as.numeric(Group),
                                 Type=as.character(container_type),
                                 QTY=as.numeric(container_qty),
                                 Size=as.numeric(container_size),
                                 Lifts="",
                                 'Charge Code'=as.character(Charge_Code),
                                 Type=as.character(charge_type),
                                 Method=as.character(charge_method),
                                 'Charge Rate'=as.numeric(Charge_Rate),
                                 'New Rate'=as.numeric(New_Charge_Rate),
                                 'Effective Date'=format(strptime(as.character(PI_Effective_Date), "%Y-%m-%d"),"%m/%d/%Y"),
                                 'Trans Code'="3",
                                 'Reason Code'="64")]
        
        upload_file_com <- test_com[.N>0,list(Index=as.numeric(Index),
                                         Comp=sprintf("%03.f", as.numeric(Division)),
                                         #Acct=sprintf("%07.f", as.numeric(Acct_nbr)),
                                         Acct=as.numeric(Acct_nbr),
                                         #Site=as.character(Site_nbr),
                                         Site=sprintf("%05.f", as.numeric(Site_nbr)),
                                         CGrp=as.numeric(Group),
                                         Type=as.character(container_type),
                                         QTY=as.numeric(container_qty),
                                         Size=as.numeric(container_size),
                                         Lifts="",comp="",
                                         'Charge Code'=as.character(Charge_Code),
                                         Type=as.character(charge_type),
                                         Method=as.character(charge_method),
                                         'Charge Rate'=as.numeric(Charge_Rate),
                                         'New Rate'=ifelse(as.numeric(New_Charge_Rate) > as.numeric(Charge_Rate),as.numeric(New_Charge_Rate),as.numeric(Charge_Rate)),
                                         'Effective Date'=format(strptime(as.character(PI_Effective_Date), "%Y-%m-%d"),"%m/%d/%Y"),
                                         'Trans Code'="3",
                                         'Reason Code'="64",
                                         'NA'="N",
                                         UOM="",
                                         WMT="")]
        
        #test_res <- test_res[as.numeric(Charge_Rate) < as.numeric(New_Charge_Rate)]
        #test_com <- test_com[as.numeric(Charge_Rate) < as.numeric(New_Charge_Rate)]
        #cat(paste0(nrow(test_com)+nrow(test_res),"\n"))
        if(count == 1){
          final_upload_file_res <- copy(upload_file_res)
          final_upload_file_com <- copy(upload_file_com)
          all_data_res <- copy(test_res)
          all_data_com <- copy(test_com)
        }else{
          l <- list(final_upload_file_res,upload_file_res)
          l2 <- list(final_upload_file_com,upload_file_com)
          m <- list(all_data_res,test_res)
          m2 <- list(all_data_com,test_com)
          final_upload_file_res <- rbindlist(l)
          final_upload_file_com <- rbindlist(l2)
          all_data_res <- rbindlist(m)
          all_data_com <- rbindlist(m2)
        }
        cat(paste0(nrow(final_upload_file_res)+nrow(final_upload_file_com),"\n"))
      }
    }
  }
  
  #all_data_res[,`:=`(NRD=as.Date(as.integer(NRD), origin="1899-12-30"))]
  #all_data_com[,`:=`(NRD=as.Date(as.integer(NRD), origin="1899-12-30"))]
  
  file_name2 <- list.files(path = paste0("D:/Share/YMP Resi/Area Files/Final/",
                                        returned_files,"/southeast/"),
                          pattern = paste0("^(.|\n)*(atlanta|Atlanta)(.|\n)*\\.xlsb$"),
                          full.names = FALSE)
  
  subDir <- paste0("D:/Share/YMP Resi/Area Files/Final/",returned_files,"/southeast/",file_name2)
  if (file.exists(subDir)){
    test_atlanta <- data.table(xl.read.file(subDir,header = FALSE, xl.sheet = "Data", top.left.cell = "A10"))
    
    setnames(test_atlanta,colnames(test_atlanta)[1:58],
             c('Index2','Region_Desc','Area_Desc','BU','Lawson_Company','Division','Acct_nbr','Site_nbr','Group',
               'Info_Div_Desc','Stop_Code','Description','Rate_Type','Rate_Description','Residential_District',
               'Residential_Description','Charge_Code','RCF','Monthly_Rate','New_Monthly_Rate','Sugg_Monthly_Rate_Rounded',
               'Increase_percent','Last_PI_per_Effective_Date','Charge_Rate','active_cust',
               'Current_Monthly_Revenue','Proposed_Monthly_Revenue','Monthly_Increase_in_Revenue',
               'Enter_New_Monthly_Rate','New_Charge_Rate','PI_Effective_Date','Comments',
               'Variance_to_Monthly_Rate','Percent_of_PI','Current_Revenue_PriortoIncrease',
               'Monthly_Revenue_After_Increase','Monthly_Revenue_Increase','Base_Rev_that_was_NOT_increased',
               'Base_Rev_that_received_an_increase','change_in_rate','NRD','frf_flag','erf_flag','admin_flag',
               'special_handling_1','special_handling_2','route_note_1','route_note_2','rate_restrict_dt',
               'disposal_rate_restrict_dt','revenue_distribution_cd','Index','container_type','container_qty',
               'container_size','service_freq','charge_type','charge_method'))
    
    test_atlanta <- test_atlanta[!is.na(Index)]
    
    test_atlanta[,`:=`(rate_restrict_dt=as.character(rate_restrict_dt),
               disposal_rate_restrict_dt=as.character(disposal_rate_restrict_dt),
               PI_Effective_Date=as.Date(PI_Effective_Date))]
    
    test_atlanta_com <- test_atlanta[Stop_Code=="cfr"]
    test_atlanta_res <- test_atlanta[Stop_Code!="cfr"]
    
    test_atlanta_com[,PI_Effective_Date:=as.Date(pi_effec_date_com,format="%Y-%m-%d")]
    
    upload_file_atlanta_res <- test_atlanta_res[!is.na(Index) & as.numeric(Charge_Rate) < as.numeric(New_Charge_Rate),
                                                list(Index=as.numeric(Index),
                                           Comp=sprintf("%03.f", as.numeric(Division)),
                                           #Acct=sprintf("%07.f", as.numeric(Acct_nbr)),
                                           Acct=as.numeric(Acct_nbr),
                                           #Site=as.character(Site_nbr),
                                           Site=sprintf("%05.f", as.numeric(Site_nbr)),
                                           CGrp=as.numeric(Group),
                                           Type=as.character(container_type),
                                           QTY=as.numeric(container_qty),
                                           Size=as.numeric(container_size),
                                           Lifts="",
                                           'Charge Code'=as.character(Charge_Code),
                                           Type=as.character(charge_type),
                                           Method=as.character(charge_method),
                                           'Charge Rate'=as.numeric(Charge_Rate),
                                           'New Rate'=as.numeric(New_Charge_Rate),
                                           'Effective Date'=format(strptime(as.character(PI_Effective_Date), "%Y-%m-%d"),"%m/%d/%Y"),
                                           'Trans Code'="3",
                                           'Reason Code'="64")]
    
    upload_file_atlanta_com <- test_atlanta_com[.N>0,list(Index=as.numeric(Index),
                                          Comp=sprintf("%03.f", as.numeric(Division)),
                                          #Acct=sprintf("%07.f", as.numeric(Acct_nbr)),
                                          Acct=as.numeric(Acct_nbr),
                                          #Site=as.character(Site_nbr),
                                          Site=sprintf("%05.f", as.numeric(Site_nbr)),
                                          CGrp=as.numeric(Group),
                                          Type=as.character(container_type),
                                          QTY=as.numeric(container_qty),
                                          Size=as.numeric(container_size),
                                          Lifts="",comp="",
                                          'Charge Code'=as.character(Charge_Code),
                                          Type=as.character(charge_type),
                                          Method=as.character(charge_method),
                                          'Charge Rate'=as.numeric(Charge_Rate),
                                          'New Rate'=ifelse(as.numeric(New_Charge_Rate) > as.numeric(Charge_Rate),as.numeric(New_Charge_Rate),as.numeric(Charge_Rate)),
                                          'Effective Date'=format(strptime(as.character(PI_Effective_Date), "%Y-%m-%d"),"%m/%d/%Y"),
                                          'Trans Code'="3",
                                          'Reason Code'="64",
                                          'NA'="N",
                                          UOM="",
                                          WMT="")]
    
    test_atlanta_res[,NRD:=as.Date(NRD)]
    test_atlanta_com[,NRD:=as.Date(NRD)]
    
    l <- list(final_upload_file_res,upload_file_atlanta_res)
    m <- list(all_data_res,test_atlanta_res)
    l2 <- list(final_upload_file_com,upload_file_atlanta_com)
    m2 <- list(all_data_com,test_atlanta_com)
    final_upload_file_res <- rbindlist(l)
    all_data_res <- rbindlist(m)
    final_upload_file_com <- rbindlist(l2)
    all_data_com <- rbindlist(m2)
  }
  
  setkey(final_upload_file_res,Index)
  setkey(final_upload_file_com,Index)
  setkey(all_data_res,Index)
  setkey(all_data_com,Index)
  
  final_upload_file_res[,Size:=round(Size,2)]
  final_upload_file_com[,Size:=round(Size,2)]
  
  if(apt_analysis){
    apt_file <- as.data.table(read.table(file = paste0("D:/Share/YMP Resi/Area Files/Final/",returned_files,"/APT analysis/APT_res_analysis.csv"),
                                         sep = ",",header = FALSE))
  
    apt_file[,c("del","Comp","Acct"):=tstrsplit(V1,split="_")]
    apt_file[,`:=`(Comp=sprintf("%03.f", as.numeric(Comp)),Acct=as.numeric(Acct),apt_inc=V2,del=NULL,V1=NULL,V2=NULL)]
    
    cols <- names(final_upload_file_res) == "Type"
    names(final_upload_file_res)[cols] <- paste0("Type", seq.int(sum(cols)))
    
    setnames(final_upload_file_res,c("Type1","Type2"),c("Type","type"))
    
    upld_colorder <- colnames(final_upload_file_res)
    summ_colorder <- colnames(all_data_res)
    
    setkey(apt_file,Comp,Acct)
    setkey(final_upload_file_res,Comp,Acct)
    
    final_upload_file_res <- apt_file[final_upload_file_res]
    final_upload_file_res[,apt_inc:=ifelse(is.na(apt_inc),0,apt_inc)]
    final_upload_file_res[,`:=`(`New Rate`=`Charge Rate`+ `Charge Rate`*(((`New Rate`-`Charge Rate`)/`Charge Rate`)+apt_inc),
                                apt_inc=NULL)]
    
    setcolorder(final_upload_file_res,upld_colorder)
    
    setnames(apt_file,c("Comp","Acct"),c("Division","Acct_nbr"))
    
    apt_file[,`:=`(Division=as.numeric(Division),Acct_nbr=as.character(Acct_nbr))]
    
    setkey(apt_file,Division,Acct_nbr)
    setkey(all_data_res,Division,Acct_nbr)
    
    all_data_res <- apt_file[all_data_res]
    
    all_data_res[,apt_inc:=ifelse(is.na(apt_inc),0,apt_inc)]
    
    all_data_res[,`:=`(apt_Percent_of_PI=Percent_of_PI+apt_inc,
                       apt_New_Charge_Rate=Charge_Rate*(1 + (Percent_of_PI+apt_inc)))]
    
    setcolorder(all_data_res,c(summ_colorder,"apt_inc","apt_Percent_of_PI","apt_New_Charge_Rate"))
    
  }
  
  write.table(final_upload_file_res, 
              file = paste0("D:/Share/YMP Resi/Area Files/Final/",returned_files,"/consolidated_res.csv"), 
              sep = ",",na = "",row.names=FALSE)
  
  write.table(all_data_res, 
              file = paste0("D:/Share/YMP Resi/Area Files/Final/",returned_files,"/all_data_res.csv"), 
              sep = ",",na = "",row.names=FALSE)
  
  write.table(final_upload_file_com, 
              file = paste0("D:/Share/YMP Resi/Area Files/Final/",returned_files,"/consolidated_com.csv"), 
              sep = ",",na = "",row.names=FALSE)
  
  write.table(all_data_com, 
              file = paste0("D:/Share/YMP Resi/Area Files/Final/",returned_files,"/all_data_com.csv"), 
              sep = ",",na = "",row.names=FALSE)
  
  shell('powershell kill -processname excel')
}