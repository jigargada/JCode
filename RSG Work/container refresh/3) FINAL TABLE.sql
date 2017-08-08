/*join rates to inv-services*/
select top 1 * from ##inv_srvc
drop table ##finalcr ; 
go 

select 
coalesce(i.month_dt, r.month_dt) as month_dt
,coalesce(i.infopro_div_nbr, r.cccomp) as infopro_div_nbr
,coalesce(i.cur_div_nbr, r.cur_div_nbr) as cur_div_nbr
,coalesce(i.acct_nbr , r.ccacct) as acct_nbr
,coalesce(i.site_nbr , r.ccsite) as site_nbr
,coalesce(i.container_grp_nbr, r.ccctgr) as container_grp_nbr
,coalesce(i.is_franchise, r.is_franchise) as is_franchise
,coalesce(i.is_national_account, r.is_national_account) as is_national_account
,coalesce(i.container_cnt, r.container_cnt) as container_cnt
,coalesce(i.inv_rev,0) as inv_rev
,coalesce(i.credit_rev,0) as credit_rev
,coalesce(i.BCR_rev_inv,0) as BCR_rev_inv
,coalesce(i.BSP_rev_inv,0) as BSP_rev_inv
,coalesce(i.BCE_Rev_inv,0) as BCE_Rev_inv
,coalesce(i.BCS_Rev_inv,0) as BCS_Rev_inv
,coalesce(i.NCC_Rev_inv,0) as NCC_Rev_inv
,coalesce(i.CEX_Rev_inv,0) as CEX_Rev_inv
,coalesce(i.EXC_Rev_inv,0) as EXC_Rev_inv
,coalesce(i.CEX_qty_bill_inv,0) as CEX_qty_bill_inv
,coalesce(i.EXC_qty_bill_inv,0) as EXC_qty_bill_inv
,coalesce(i.NCC_qty_bill_inv,0) as NCC_qty_bill_inv
,coalesce(i.NCC_Rate_inv,0) as NCC_Rate_inv
,coalesce(i.CEX_Rate_inv,0) as CEX_Rate_inv
,coalesce(i.EXC_Rate_inv,0) as EXC_Rate_inv
,coalesce(i.CEX_cnt_srvc,0) as CEX_cnt_srvc
,coalesce(i.NCC_cnt_srvc,0) as NCC_cnt_srvc
,coalesce(i.EXC_cnt_srvc,0) as EXC_cnt_srvc 
,coalesce(r.cont_close_mo, i.cont_close_mo) as cont_close_mo
,coalesce(r.cont_start_mo, i.cont_start_mo) as cont_start_mo
,coalesce(r.act_open_mo, i.act_open_mo) AS act_open_mo
,COALESCE(r.act_close_mo, I.act_close_mo) AS act_close_mo
,COALESCE(r.container_size, I.container_size) AS container_size
,coalesce(r.bcr_rt,0) as bcr_rt
,coalesce(r.bcs_rt,0) as bcs_rt
,coalesce(r.bcr_active_cont_bom,0) as bcr_active_cont_bom
,coalesce(r.bcr_active_cont_eom,0) AS bcr_active_cont_eom
,coalesce(r.bcr_new_cnt,0) as bcr_new_count
,coalesce(r.bcr_lost_cnt,0) as bcr_lost_count

,COALESCE(r.bcr_exists, 0) as bcr_exists
,coalesce(r.bce_Exists,0) as bce_Exists
,coalesce(r.bsp_exists,0) as bsp_exists
,coalesce(r.bcs_exists,0) as bcs_exists
,COALESCE(r.new_acct, I.NEW_aCT) AS new_acct
,coalesce(r.lost_acct, i.lost_act) as lost_acct
,coalesce(r.closed_container, i.closed_container) as closed_container
,coalesce(r.open_container , i.open_container) as open_container 
,coalesce(i.FRF_rate_pct, r.frf_Rate_pct) as FRF_rate_pct
,coalesce(i.ERF_rate_pct, r.erf_Rate_pct) as ERF_rate_pct
,coalesce(i.is_ERF_Locked, r.is_ERF_Locked) as is_ERF_Locked
,coalesce(i.is_FRF_Locked, r.is_FRF_Locked) as is_FRF_Locked
,coalesce(i.is_ERF_on_FRF, r.is_ERF_on_FRF) as is_ERF_on_FRF

into ##finalcr
from ##inv_srvc as i
full join ##temp_new1 as r 
on i.infopro_Div_nbr = r.cccomp
and i.acct_nbr = r.ccacct
and i.site_nbr = r.ccsite
and i.container_grp_nbr = r.ccctgr 
and i.month_dt = r.month_dt 
and i.cur_div_nbr = r.cur_div_nbr


print 'finalcr done'

/*create account fee and join fees to ##finalcr in #finalcr2*/

DROP TABLE #ACCT_FEE;
drop table #finalcr2;
drop table #hier ;
drop table ##finalcr3 ; 
GO

select 
f.Infopro_Div_Nbr
 ,f.Acct_Nbr 
 ,max(case when daf.Fee_Type = 'FRF' and daf.is_Fee_Charged=1 and daf.is_Acct_Fee_Enabled=1  then 1 else 0 end)  as FRF_charged
 ,max(case when daf.Fee_Type = 'EVR' and daf.is_Fee_Charged=1 and daf.is_Acct_Fee_Enabled=1 then 1 else 0 end)  as ERF_charged 
 INTO #ACCT_fee
from ##finalcr f 
join dwcore.dbo.dim_acct_fee daf
on daf.infopro_div_nbr = f.infopro_div_nbr
and daf.Acct_Nbr = f.acct_nbr 
and is_current = 1  
 where daf.Fee_Type in ('FRF','EVR')
 group by f.Infopro_Div_Nbr ,f.Acct_Nbr
 
 print 'acctfee done'

SELECT  
F.* 
,AF.ERF_charged
,AF.FRF_charged
,ddf.FRF_RATE*ddf.Is_FRF_Enabled/100 as full_frf
,ddf.ERF_RATE*ddf.Is_ERF_Enabled/100 AS  Full_ERF

,case  when coalesce(is_erf_locked,0) = 1 then erf_rate_pct*coalesce(af.ERF_charged,1)*coalesce(ddf.is_erf_enabled,1)
       when coalesce(is_erf_locked ,0) = 0 then ddf.ERF_RATE*ddf.Is_ERF_Enabled/100*coalesce(erf_charged ,1)  end as erf_rate

,case  when coalesce(is_frf_locked ,0) = 1 then frf_rate_pct*coalesce(ddf.is_frf_enabled,1)*coalesce(af.frf_charged ,1) 
       when coalesce(is_frf_locked,0)  = 0 then ddf.fRF_RATE*ddf.Is_FRF_Enabled /100 *coalesce(frf_charged ,1)  end as frf_rate

,coalesce(is_erf_on_frf,0) as is_erf_on_frf_2
into #finalcr2
FROM ##FINALCR F 
left JOIN #ACCT_FEE AF
ON F.Acct_Nbr = aF.ACCT_NBR
AND F.Infopro_Div_Nbr = aF.INFOPRO_DIV_NBR

JOIN DWCORE.DBO.Dim_Division_Fee DDF
ON DDF.Monthly_Pd_SK = F.MONTH_DT
AND DDF.Infopro_Div_Nbr = F.INFOPRO_DIV_NBR 

print 'finalcr2 done acct fee and div fee joined ' 

/*create hierarchy*/

SELECT DISTINCT 
Cur_Region_Nm
,Cur_Area_Nbr
,Cur_Area_Nm
,CUR_DIV_NBR
,CUR_DIV_NM
,CUR_BU_NBR + ' ' + CUR_BU_DESC AS BU_NAME
,Cur_Infopro_Div_Nbr
,Cur_Infopro_Div_Nm
INTO #HIER
FROM DWCORE.DBO.Dim_Corp_Hier

WHERE IS_CURRENT =1 
AND (CUR_LOB_CATEGORY LIKE '%cOMMERCIAL%' OR Cur_LOB_Category LIKE '%INDUSTRIAL%' ) AND Cur_Area_Nm NOT LIKE 'DIVESTED'
order by cur_Infopro_Div_Nbr
print 'hier done'

/*join hierarchy and #finalcr2 to create final table, do revenue calcs*/

select 
month_dt
,Cur_Region_Nm
,Cur_Area_Nbr
,Cur_Area_Nm
,H.CUR_DIV_NBR
,H.CUR_DIV_NM
,BU_NAME
,Cur_Infopro_Div_Nm
,infopro_Div_nbr
,acct_nbr 
,site_nbr
,container_grp_nbr
,is_franchise
,is_national_account
,bcr_rev_inv+bsp_rev_inv+bce_rev_inv+bcs_rev_inv as bcr_rev_inv_base
,((is_erf_on_frf_2)*(1+FRF_RATE)*(1+ERF_RATE)+(1-is_erf_on_frf_2)*(1+ERF_RATE+FRF_RATE))*(bcr_rev_inv+bsp_rev_inv+bce_rev_inv+bcs_rev_inv) as bcr_rev_inv_fees
,ncc_rev_inv 
,cex_rev_inv
,exc_rev_inv
,ncc_RATE_INV
,CEX_RATE_INV
,EXC_RATE_INV
,NCC_qty_bill_inv
,CEX_qty_bill_inv
,EXC_qty_bill_inv
,((is_erf_on_frf_2)*(1+FRF_RATE)*(1+ERF_RATE)+(1-is_erf_on_frf_2)*(1+ERF_RATE+FRF_RATE))*ncc_RATE_INV as ncc_rate_inv_fees
,((is_erf_on_frf_2)*(1+FRF_RATE)*(1+ERF_RATE)+(1-is_erf_on_frf_2)*(1+ERF_RATE+FRF_RATE))*cex_rATE_inv as cex_rate_inv_fees
,((is_erf_on_frf_2)*(1+FRF_RATE)*(1+ERF_RATE)+(1-is_erf_on_frf_2)*(1+ERF_RATE+FRF_RATE))*exc_rATE_inv as exc_rate_inv_fees
,((is_erf_on_frf_2)*(1+FRF_RATE)*(1+ERF_RATE)+(1-is_erf_on_frf_2)*(1+ERF_RATE+FRF_RATE))*ncc_rev_inv  as ncc_rev_inv_fees
,((is_erf_on_frf_2)*(1+FRF_RATE)*(1+ERF_RATE)+(1-is_erf_on_frf_2)*(1+ERF_RATE+FRF_RATE))*CEX_REV_INV as cex_rev_inv_fees
,((is_erf_on_frf_2)*(1+FRF_RATE)*(1+ERF_RATE)+(1-is_erf_on_frf_2)*(1+ERF_RATE+FRF_RATE))*exc_rev_inv as exc_rev_inv_fees

,CEX_CNT_SRVC
,NCC_CNT_SRVC
,EXC_CNT_SRVC
,(BCR_RT) * CONTAINER_CNT  AS BCR_RT_base
,(BCS_RT) * CONTAINER_CNT  AS BCS_RT_base
,((is_erf_on_frf_2)*(1+FRF_RATE)*(1+ERF_RATE)+(1-is_erf_on_frf_2)*(1+ERF_RATE+FRF_RATE))*(BCR_RT) * CONTAINER_CNT AS BCR_RT_FEES
,((is_erf_on_frf_2)*(1+FRF_RATE)*(1+ERF_RATE)+(1-is_erf_on_frf_2)*(1+ERF_RATE+FRF_RATE))*(BCS_RT) * CONTAINER_CNT AS BCS_RT_FEES
,BCR_ACTIVE_CONT_BOM
,BCR_ACTIVE_CONT_EOM 
,bcr_new_count 
,bcr_lost_count
,CASE WHEN BCR_EXISTS+BCE_EXISTS+BCS_EXISTS+BSP_EXISTS >0 THEN 1 ELSE 0 END AS BCR_EXISTS
,(is_erf_on_frf_2)*(1+FRF_RATE)*(1+ERF_RATE)+(1-is_erf_on_frf_2)*(1+ERF_RATE+FRF_RATE)-1 as fees_pct
,NEW_ACCT
,LOST_ACCT
,CLOSED_CONTAINER
,OPEN_CONTAINER 
,CASE WHEN NEW_ACCT =1  AND LOST_ACCT = 0 THEN 'NEW ACCOUNT'
WHEN LOST_ACCT = 1 THEN 'LOST ACCOUNT'
WHEN NEW_ACCT =0  AND LOST_ACCT = 0  AND OPEN_CONTAINER = 1  AND CLOSED_CONTAINER = 0 THEN 'EXISTING - NEW CONTAINER'
WHEN NEW_ACCT =0  AND LOST_ACCT = 0  AND CLOSED_CONTAINER = 1  THEN 'EXISTING - CLOSED CONTAINER'
WHEN NEW_ACCT =0  AND LOST_ACCT = 0  AND CLOSED_CONTAINER = 0 AND OPEN_CONTAINER = 1  THEN 'EXISTING - OPEN CONTAINER'
WHEN NEW_ACCT =0  AND LOST_ACCT = 0  AND OPEN_CONTAINER = 0 AND CLOSED_CONTAINER = 0  THEN 'EXISTING - NO CHANGE'
END AS ACCT_STATUS 

 into ##finalcr3
 from #finalcr2
 JOIN #HIER  H
 ON H.Cur_Div_Nbr = #FINALCR2.CUR_DIV_NBR
 AND H.Cur_Infopro_Div_Nbr = #FINALCR2.INFOPRO_DIV_NBR 
 
 print 'finalcr3 done her joined fees calc'

/*final summmarization*/
 SELECT 
month_dt
,Cur_Region_Nm
,Cur_Area_Nbr
,Cur_Area_Nm
,CUR_DIV_NBR
,CUR_DIV_NM
,BU_NAME
,Cur_Infopro_Div_Nm
,infopro_Div_nbr
,is_franchise
,is_national_account
,ACCT_STATUS
,BCR_EXISTS
,SUM(bcr_rev_inv_base) AS bcr_rev_inv_base
,SUM(bcr_rev_inv_fees) as bcr_rev_inv_fees
,sum(ncc_rev_inv ) as ncc_rev_inv_base
,sum(cex_rev_inv) as cex_rev_inv_base
,sum(exc_rev_inv) as exc_rev_inv_base
,sum(ncc_RATE_INV) as ncc_RATE_INV_base
,sum(CEX_RATE_INV) as CEX_RATE_INV_base
,sum(EXC_RATE_INV) as EXC_RATE_INV_base
,sum(ncc_rate_inv_fees) as ncc_rate_inv_fees
,sum(cex_rate_inv_fees) as cex_rate_inv_fees
,sum(exc_rate_inv_fees) as exc_rate_inv_fees
,sum(ncc_rev_inv_fees) as ncc_rev_inv_fees
,sum(cex_rev_inv_fees) as cex_rev_inv_fees
,sum(exc_rev_inv_fees) as exc_rev_inv_fees
,SUM(CEX_qty_bill_inv) AS cex_qty_bill_inv
,SUM(exc_qty_bill_inv) AS exc_qty_bill_inv
,SUM(ncc_qty_bill_inv) AS ncc_qty_bill_inv
,sum(CEX_CNT_SRVC) as CEX_CNT_SRVC
,sum(NCC_CNT_SRVC) as NCC_CNT_SRVC
,sum(EXC_CNT_SRVC) as EXC_CNT_SRVC
,sum(BCR_RT_base) as BCR_RT_base
,sum(BCS_RT_base) as BCS_RT_base
,sum(BCR_RT_FEES) as BCR_RT_FEES
,sum(BCS_RT_FEES) as BCS_RT_FEES
,sum(BCR_ACTIVE_CONT_BOM) as BCR_ACTIVE_CONT_BOM
,sum(BCR_ACTIVE_CONT_EOM ) as BCR_ACTIVE_CONT_EOM 
,sum(bcr_new_count) as BCR_New_Cnt
,sum(bcr_LOST_count) as BCR_lost_Cnt
--,fees_pct
--,NEW_ACCT
--,LOST_ACCT
--,CLOSED_CONTAINER
--,OPEN_CONTAINER 
FROM ##FINALCR3 
group by month_dt
,Cur_Region_Nm
,Cur_Area_Nbr
,Cur_Area_Nm
,CUR_DIV_NBR
,CUR_DIV_NM
,BU_NAME
,Cur_Infopro_Div_Nm
,infopro_Div_nbr
,is_franchise
,is_national_account
,ACCT_STATUS
,BCR_EXISTS