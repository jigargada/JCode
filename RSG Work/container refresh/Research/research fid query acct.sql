select top 1000 

fid.Revenue_Period_SK
,Charge_Cd
,Rate_Amt
,Revenue_Amt
,is_credit
,Qty_Billed

,*
 from dwcore.dbo.fact_invoice_Detail fid 
join dwcore.dbo.Dim_Container_Grp dcg
on fid.container_grp_sk_2 = dcg.Container_Grp_SK_2
join dwcore.dbo.dim_charge_cd dcc
on fid.Charge_Cd_SK = dcc.charge_cd_sk
join dwcore.dbo.dim_rate_hist drh
on drh.rate_sk  = fid.rate_sk 
--incorrect fee rev in fid
and dcg.acct_nbr = '52715' and dcg.infopro_div_nbr  = '239'
and fid.Revenue_Period_SK<=201605 and fid.Revenue_Period_SK>=201603 
and fid.is_Deleted = 0 and fid.is_updated = 0 

order by dcg.infopro_div_nbr, dcg.acct_nbr , dcg.site_nbr , dcg.container_Grp_nbr , fid.Revenue_Period_SK

