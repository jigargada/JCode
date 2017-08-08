--drop table ##temp_new1
--go
/*run for each month changing date @month below*******************************/ 
/*for first month, uncomment drop table and into, comment insert into ********/
/*for all other  months, comment drop table and into, uncomment insert into */
DECLARE @cnt INT = 1, @month as datetime,   @monthint as int, @monthcl as datetime, @monthintend as int;

WHILE @cnt <= 2
BEGIN
  
 
set @month = '2017-01-01 00:00:00.000' 
set @monthcl = dateadd(month, @cnt, @month)
set @monthint= year(@month)*10000+month(@month)*100+1
set @monthintend=year(@monthcl)*10000+month(@monthcl)*100+1; 


/*TEMP_NEW:  active rates with BCR*/ 
INSERT INTO ##TEMP_NEW1 
SELECT
floor(@monthint/100) as month_Dt
,r.[CCCOMP] 
,r.[CCACCT]
,dch.Cur_Div_Nbr 
,r.[CCSITE]
,r.[CCCTGR]
,year(dcg.Close_Dt)*100+ month(dcg.Close_Dt) as cont_close_mo
,year(dcg.Orig_Start_Dt)*100+month(dcg.Orig_Start_Dt) as cont_start_mo
,year(da.acct_close_dt)*100+month(da.acct_close_dt) as act_close_mo
,year(da.acct_open_dt)*100+month(da.acct_open_dt) as act_open_mo
,max(case when year(da.acct_open_dt)*100+month(da.acct_open_dt) = floor(@monthint/100) then 1 else 0 end) as new_acct
,max(case when year(da.acct_close_dt)*100+month(da.acct_close_dt) = floor(@monthint/100) then 1 else 0 end) as lost_acct	  
,max(case when year(dCG.Close_Dt)*100+month(dcg.Close_Dt) = floor(@monthint/100) then 1 else 0 end) as closed_container
,min(case when year(dCG.Orig_Start_Dt)*100+month(dcg.Orig_Start_Dt) = floor(@monthint/100) then 1 else 0 end) as open_container	  

,dcg.Container_Cnt
,dcg.Container_Size
,dcg.is_franchise
,da.is_National_Account
,dcf.ERF_Rate_Pct/100 AS erf_RATE_PCT
,dcf.FRF_Rate_Pct/100 AS FRF_RATE_PCT
,dcf.Is_ERF_Locked
,dcf.Is_FRF_Locked
,dcf.Is_ERF_On_FRF

,sum(case when r.ccchcd in ( 'BCR', 'BSP', 'BCE')  and r.CCEFDT<= @monthint and (r.cccldt> @monthint or r.cccldt=0 ) and da.Acct_Open_Dt<@month and dcg.Orig_Start_Dt<@month  and da.acct_close_dt >@month and dcg.close_dt> @month  then r.[CCCHRT] else 0 end) as BCR_RT	  
,sum(case when r.ccchcd in               ( 'BCS')  and r.CCEFDT<= @monthint and (r.cccldt> @monthint or r.cccldt=0 ) and da.Acct_Open_Dt<@month and dcg.Orig_Start_Dt<@month  and da.acct_close_dt >@month and dcg.close_dt> @month  then r.[CCCHRT] else 0 end) as BCS_RT	  
,max(case when r.ccchcd in ( 'BCR', 'BSP', 'BCE')  and r.ccefdt <= @monthint and (r.CCCLDT> @monthint or r.cccldt=0) and da.Acct_Open_Dt<=@month and dcg.Orig_Start_Dt<=@month and da.acct_close_dt >@month and dcg.close_dt> @month then dcg.container_Cnt  else 0 end) as BCR_Active_cont_bom 
,max(case when r.ccchcd in ( 'BCR', 'BSP', 'BCE') and r.ccefdt <= @monthintend and (r.CCCLDT>=@monthintend or r.cccldt=0) and da.Acct_open_Dt<=@monthcl and dcg.orig_start_Dt<=@monthcl and da.acct_close_dt >@monthcl and dcg.close_dt> @monthcl  then dcg.container_Cnt  else 0 end) as BCR_Active_cont_eom 
,max(case when                                        floor(r.ccefdt/100)= floor(@monthint/100) and r.ccchcd in ( 'BCR', 'BSP', 'BCE') and da.Acct_Open_Dt<=@month and dcg.Orig_Start_Dt<=@month and da.Acct_Close_Dt>@month and dcg.Close_Dt>@month then dcg.container_Cnt  else 0 end) as BCR_new_cnt
,max(case when                                        floor(r.cccldt/100)= floor(@monthint/100) and r.ccchcd in ( 'BCR', 'BSP', 'BCE') and da.Acct_Open_Dt<=@month and dcg.Orig_Start_Dt<=@month and da.Acct_Close_Dt>@month and dcg.Close_Dt>@month then dcg.container_Cnt  else 0 end) as BCR_lost_cnt
,max(case when r.ccchcd = 'BCR' then DCG.CONTAINER_CNT else 0 end) as BCR_exists
,max(case when r.ccchcd = 'BCE' then DCG.CONTAINER_CNT  else 0 end) as BCE_exists
,max(case when r.ccchcd = 'BSP' then DCG.CONTAINER_CNT  else 0 end) as BSP_exists
,max(case when r.ccchcd = 'BCS' then DCG.CONTAINER_CNT  else 0 end) as BCS_exists

--iNTO ##TEMP_NEW1
  FROM [DWPSA].[dbo].[STG_IFP_BIPCC_2] r
  join dwcore.dbo.Dim_Container_Grp dcg 
  on r.CCCOMP = dcg.Infopro_Div_Nbr
  and r.ccacct = dcg.Acct_Nbr
  and r.ccsite = dcg.Site_Nbr
  and r.CCCTGR = dcg.Container_Grp_Nbr

  join dwcore.dbo.dim_acct da
  on r.ccacct = da.acct_nbr
  and r.cccomp = da.infopro_div_nbr
     
   join DWCORE.dbo.Dim_Corp_Hier dch
  on dch.Cur_Infopro_Div_Nbr = dcg.Infopro_Div_Nbr
  and dch.Rev_Distrib_Cd = dcg.Revenue_Distribution_Cd

  left join dwcore.dbo.Dim_Contract_Fee dcf
  on dcg.Infopro_Div_Nbr = dcf.Infopro_Div_Nbr
  and dcg.Contract_Nbr = dcf.Contract_Nbr
  and dcg.Contract_Grp_Nbr = dcf.Contract_Grp_Nbr
  and dcf.eff_dt <= @monthcl
  and dcf.disc_dt >= @monthcl

    where r.CCCHCD in ('BCR', 'BSP', 'BCE', 'BCS')
  and da.is_current = 1
  and dch.is_current = 1 
  and dcg.is_current=1
  --and dcg.eff_dt<=@monthCL
  --and dcg.disc_dt>@monthCL 
  --accounts have to be open during the month
  
  and dcg.Orig_Start_Dt<@monthcl  
  and da.Acct_Open_Dt <@monthcl
  and [CCEFDT] < @monthintend
  and dcg.Close_Dt>@month
  and da.Acct_Close_Dt>@month 
  and ([CCCLDT] > @monthint or  [CCCLDT] = 0 )
  and convert(datetime, convert(char(8) , ccefdt))<da.Acct_Close_Dt  
  and convert(datetime, convert(char(8) , ccefdt))<dcg.Close_Dt
  and (r.cccldt>= r.ccefdt or r.cccldt=0)    
  and R.current_ind = 'y' 
   and R.CCBADJ= '0' 
     	   
	group by  
r.[CCCOMP] 
,r.[CCACCT]
,dch.Cur_Div_Nbr 
,r.[CCSITE]
,r.[CCCTGR]
,year(dcg.Close_Dt)*100+ month(dcg.Close_Dt)
,year(dcg.Orig_Start_Dt)*100+month(dcg.Orig_Start_Dt)
,year(da.acct_close_dt)*100+month(da.acct_close_dt)
,year(da.acct_open_dt)*100+month(da.acct_open_dt)
,dcg.Container_Cnt
,dcg.Container_Size
,dcg.is_franchise
,da.is_National_Account
,dcf.ERF_Rate_Pct/100
,dcf.FRF_Rate_Pct/100
,dcf.Is_ERF_Locked
,dcf.Is_FRF_Locked
,dcf.Is_ERF_On_FRF

   SET @cnt = @cnt + 1;
END;

/*
select 
month_dt
,sum(BCR_Active_cont_bom) as BCR_BOM
,sum(BCR_Active_cont_eom) as BCR_EOM
,sum(bcr_new_cnt) as bcr_new_cont
,sum(bcr_lost_cnt) as bcr_lost_cont
,count(*) as num
from ##TEMP_NEW1
   group by month_dt
order by month_dt
*/
