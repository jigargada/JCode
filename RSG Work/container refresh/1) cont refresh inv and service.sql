/*services and invoices*/

	/* TEMP_INVDET Temp table for Invoice Detail*/
	
	drop table ##temp_invdet 

	select 
	FID.Revenue_Period_SK
	,dcg.Infopro_Div_Nbr
	, dch.Cur_Div_Nbr
	,dcg.Acct_Nbr
	, dcg.Site_Nbr
	,dcg.Container_Grp_Nbr
	,max(DCG.Container_Cnt) as container_cnt 
	,max(dcg.Container_Size) as container_size
	,max(case when DCG.is_Franchise= 1 then 1 else 0 end) as is_Franchise
	,max(case when DA.is_National_Account=1 then 1 else 0 end) as is_national_account 
	,max(dcf.ERF_Rate_Pct/100) as erf_rate_pct
,max(dcf.FRF_Rate_Pct/100 ) as frf_rate_pct
,max(case when dcf.Is_ERF_Locked=1 then 1 else 0 end) as is_ERF_Locked
,max(case when dcf.Is_FRF_Locked=1 then 1 else 0 end) as is_FRF_Locked
,max(case when dcf.Is_ERF_On_FRF =1  then 1 else 0 end) as is_ERF_on_FRF
,min(year(dcg.Close_Dt)*100+ month(dcg.Close_Dt)) as cont_close_mo
,min(year(dcg.Orig_Start_Dt)*100+month(dcg.Orig_Start_Dt)) as cont_start_mo
,min(year(da.acct_close_dt)*100+month(da.acct_close_dt)) as act_close_mo
,min(year(da.acct_open_dt)*100+month(da.acct_open_dt) ) as act_open_mo
,(case when min(year(da.acct_open_dt)*100+month(da.acct_open_dt)) = Revenue_Period_SK then 1 else 0 end) as new_acct
,(case when min(year(da.acct_close_dt)*100+month(da.acct_close_dt)) = Revenue_Period_SK then 1 else 0 end) as lost_acct	  
,(case when min(year(dCG.Close_Dt)*100+month(dcg.Close_Dt)) = Revenue_Period_SK then 1 else 0 end) as closed_container
,(case when min(year(dCG.Orig_Start_Dt)*100+month(dcg.Orig_Start_Dt)) = Revenue_Period_SK then 1 else 0 end) as open_container	  

	,SUM(revenue_amt ) AS INV_REV
	,SUM(CASE WHEN IS_CREDIT = 1 THEN REVENUE_AMT ELSE 0 END ) AS CREDIT_REV 
	,SUM( CASE WHEN DCC.Charge_Cd = 'BCR' THEN Revenue_Amt ELSE 0 END) AS BCR_REV
	,SUM( CASE WHEN DCC.Charge_Cd = 'BSP' THEN Revenue_Amt ELSE 0 END) AS BSP_REV
	,SUM( CASE WHEN DCC.Charge_Cd = 'BCE' THEN Revenue_Amt ELSE 0 END) AS BCE_REV
	,SUM( CASE WHEN DCC.Charge_Cd = 'BCS' THEN Revenue_Amt ELSE 0 END) AS BCS_REV
	,SUM( CASE WHEN DCC.Charge_Cd = 'NCC' THEN Revenue_Amt ELSE 0 END) AS NCC_REV
	,SUM( CASE WHEN DCC.Charge_Cd = 'CEX' THEN Revenue_Amt ELSE 0 END) AS CEX_REV
	,SUM( CASE WHEN DCC.Charge_Cd = 'EXC' THEN Revenue_Amt ELSE 0 END) AS EXC_REV

	,SUM( CASE WHEN DCC.Charge_Cd = 'NCC' THEN drh.rate_amt ELSE 0 END) AS NCC_Rate
	,SUM( CASE WHEN DCC.Charge_Cd = 'CEX' THEN drh.rate_amt ELSE 0 END) AS CEX_Rate
	,SUM( CASE WHEN DCC.Charge_Cd = 'EXC' THEN drh.rate_amt ELSE 0 END) AS EXC_Rate
	,SUM( CASE WHEN DCC.Charge_Cd = 'NCC' THEN fid.Qty_Billed ELSE 0 END) AS NCC_qty_bill_inv
	,SUM( CASE WHEN DCC.Charge_Cd = 'CEX' THEN fid.Qty_Billed ELSE 0 END) AS CEX_qty_bill_inv
	,SUM( CASE WHEN DCC.Charge_Cd = 'EXC' THEN fid.Qty_Billed ELSE 0 END) AS EXC_qty_bill_inv

	
	into ##temp_invdet
	from dwcore.dbo.Fact_Invoice_Detail fid
	join dwcore.dbo.dim_acct da
	on fid.acct_sk = da.acct_sk 

	join dwcore.dbo.Dim_Container_Grp dcg
	on fid.Container_Grp_SK_2 = dcg.Container_Grp_SK_2

	join dwcore.dbo.Dim_Charge_Cd dcc
	on fid.Charge_Cd_SK=dcc.Charge_Cd_SK

	 join DWCORE.dbo.Dim_Corp_Hier dch
	  on fid.Corp_Hier_SK = dch.Corp_Hier_SK

	  join dwcore.dbo.Dim_Rate_Hist drh
	  on drh.rate_sk = fid.rate_Sk 

  left join dwcore.dbo.Dim_Contract_Fee dcf
  on dcg.Infopro_Div_Nbr = dcf.Infopro_Div_Nbr
  and dcg.Contract_Nbr = dcf.Contract_Nbr
  and dcg.Contract_Grp_Nbr = dcf.Contract_Grp_Nbr
  and dcf.is_Current = 1

	where 
	fid.is_Updated=0 and fid.is_Deleted = 0 and fid.is_Merged=0 and fid.is_Split=0 
	and dcc.Charge_Cd in ('BCR', 'BSP', 'BCE', 'NCC', 'CEX', 'EXC', 'BCS')
	AND Revenue_Period_SK>201600 
	and dcg.Container_Size>=1 and dcg.Container_Size<=10
	
	GROUP BY
	FID.Revenue_Period_SK
	,dcg.Infopro_Div_Nbr
	, dch.Cur_Div_Nbr
	,dcg.Acct_Nbr
	, dcg.Site_Nbr
	,dcg.Container_Grp_Nbr

	
	/* TEMP_SRV:  Temp table for Service Detail*/
	drop table ##temp_srv;
	go
	SELECT 
floor(fsd.Service_Dt_SK/100) as service_mo
,dcg.Infopro_Div_Nbr 
,dch.Cur_Div_Nbr 
,dcg.Acct_Nbr 
,dcg.Site_Nbr 
,dcg.Container_Grp_Nbr 

,max(DCG.Container_Cnt) as container_cnt 
,max(dcg.Container_Size) as container_size
,max(case when DCG.is_Franchise= 1 then 1 else 0 end) as is_Franchise
,max(case when DA.is_National_Account=1 then 1 else 0 end) as is_national_account 

,min(year(dcg.Close_Dt)*100+ month(dcg.Close_Dt)) as cont_close_mo
,min(year(dcg.Orig_Start_Dt)*100+month(dcg.Orig_Start_Dt)) as cont_start_mo
,min(year(da.acct_close_dt)*100+month(da.acct_close_dt)) as act_close_mo
,min(year(da.acct_open_dt)*100+month(da.acct_open_dt) ) as act_open_mo


,SUM(case when  dsc.[Service_Cd]= 'cex' then fsd.Container_Cnt else 0 end) as CEX_cnt_srvc
,SUM(case when  dsc.[Service_Cd]= 'NCC' then fsd.Container_Cnt else 0 end) as NCC_cnt_srvc
,SUM(case when  dsc.[Service_Cd]= 'EXC' then fsd.Container_Cnt else 0 end) as EXC_cnt_srvc
,max(case when year(da.acct_open_dt)*100+month(da.acct_open_dt) = floor(fsd.Service_Dt_SK/100) then 1 else 0 end) as new_acct
,max(case when year(da.acct_close_dt)*100+month(da.acct_close_dt) = floor(fsd.Service_Dt_SK/100) then 1 else 0 end) as lost_acct	  
,max(case when year(dCG.Close_Dt)*100+month(dcg.Close_Dt) = floor(fsd.Service_Dt_SK/100) then 1 else 0 end) as closed_container
,min(case when year(dCG.Orig_Start_Dt)*100+month(dcg.Orig_Start_Dt) = floor(fsd.Service_Dt_SK/100) then 1 else 0 end) as open_container	  

into ##temp_srv 
  FROM [DWCORE].[dbo].[Fact_Service_Detail] fsd 
  join [DWCORE].[dbo].[Dim_Service_Cd] dsc 
         on fsd.Service_Cd_SK = dsc.Service_Cd_SK
join dwcore.dbo.Dim_Container_Grp dcg
on dcg.Container_Grp_SK = fsd.Container_Grp_SK
join DWCORE.dbo.Dim_Corp_Hier dch
  on fsd.corp_hier_sk = dch.corp_hier_sk 
JOIN DWCORE.DBO.DIM_ACCT DA
ON DA.ACCT_SK = FSD.Acct_SK 
where dsc.Service_Cd in ('CEX', 'NCC', 'EXC')
and dcg.Container_Size<=10
and dcg.Container_Size>=1
and fsd.Service_Dt_SK >= 20160101
and fsd.is_Deleted = 0 and fsd.is_Updated=0  
group by floor(fsd.Service_Dt_SK/100) 
,dcg.Infopro_Div_Nbr
,dch.Cur_Div_Nbr
,dcg.Acct_Nbr
,dcg.Site_Nbr
,dcg.Container_Grp_Nbr

drop table ##inv_srvc
go
/*invoice and services joined*/
select 
coalesce(i.infopro_div_nbr, s.infopro_div_nbr) as infopro_div_nbr
,coalesce(i.cur_div_nbr, s.cur_div_nbr) as cur_div_nbr
,coalesce(i.Acct_Nbr, s.acct_nbr) as acct_nbr
,coalesce(i.Site_Nbr, s.site_nbr) as site_nbr
,coalesce(i.Container_Grp_Nbr, s.container_grp_nbr) as container_Grp_nbr
,coalesce(i.is_Franchise, s.is_franchise) as is_Franchise
,coalesce(i.is_National_Account, s.is_national_account) as is_national_Account 
,coalesce(i.Revenue_Period_SK, s.service_mo) as month_dt
,coalesce(i.Container_Cnt, s.container_cnt) as container_cnt
,coalesce(i.inv_rev,0) as inv_rev
,coalesce(i.CREDIT_REV,0) as credit_rev
,coalesce(i.BCR_REV,0) as BCR_REV_inv
,coalesce(i.BSP_REV,0) as BSP_REV_inv
,coalesce(i.BCE_REV,0) as BCE_REV_inv
,coalesce(i.BCS_REV,0) as BCS_REV_inv
,coalesce(i.NCC_REV,0) as NCC_REV_inv
,coalesce(i.CEX_REV,0) as CEX_REV_inv
,coalesce(i.EXC_REV,0) as EXC_REV_inv
,coalesce(i.ncc_rate,0 ) as NCC_Rate_inv
,coalesce(i.EXC_rate,0 ) as EXC_Rate_inv
,coalesce(i.CEX_rate,0 ) as CEX_Rate_inv

,coalesce(i.NCC_qty_bill_inv,0) as NCC_qty_bill_inv 
,coalesce(CEX_qty_bill_inv,0) as CEX_qty_bill_inv
,coalesce(EXC_qty_bill_inv,0) as EXC_qty_bill_inv
,coalesce(s.CEX_cnt_srvc,0) as CEX_cnt_srvc
,coalesce(s.NCC_cnt_srvc,0) as NCC_cnt_srvc
,coalesce(s.EXC_cnt_srvc,0) as EXC_cnt_srvc

,COALESCE(i.Container_Size, s.container_size) as container_size
,coalesce(i.cont_close_mo, s.cont_close_mo) as cont_close_mo
,coalesce(i.cont_start_mo, s.cont_start_mo) as cont_start_mo
,coalesce(i.act_close_mo, s.act_close_mo) as act_close_mo
,coalesce(i.act_open_mo, s.act_open_mo) as act_open_mo
,coalesce(i.new_acct, s.new_acct) as new_act
,coalesce(i.lost_acct, s.lost_acct) as lost_act
,coalesce(i.closed_container, s.closed_container) as closed_container
,coalesce(i.open_container, s.open_container) as open_container

,i.erf_rate_pct as erf_rate_pct
,i.frf_rate_pct as frf_rate_pct
,i.Is_ERF_Locked as is_erf_locked
,i.Is_FRF_Locked  as is_frf_locked
,i.Is_ERF_On_FRF as is_erf_on_frf

	
into ##inv_srvc
from ##temp_invdet i 
full join ##temp_srv s 
on i.Infopro_Div_Nbr = s.Infopro_Div_Nbr
and i.Acct_Nbr = s.Acct_Nbr
and i.Site_Nbr = s.Site_Nbr
and i.Container_Grp_Nbr = s.Container_Grp_Nbr
and i.Revenue_Period_SK = s.service_mo
and i.cur_div_nbr = s.cur_Div_nbr 


select top 1 * from ##temp_invdet
select top 1 * from ##temp_srv 
select top 1 * from ##inv_srvc
