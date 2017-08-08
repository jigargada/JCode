/*RESI EMAIL LIST QUERY*/
with accts as(
SELECT 
b.COMPANY_NUMBER AS infopro_div_nbr
,b.ACCOUNT_NUMBER as acct_nbr 
,b.SITE AS site_nbr
,b.CONTAINER_GROUP AS container_grp_nbr
,case when b.charge_method = 'Q' then b.CHARGE_RATE*dcg.Container_Cnt*dcg.Recurring_Charge_Freq 
    else b.CHARGE_RATE*dcg.Recurring_Charge_Freq end as revenue_ann
,case when b.charge_method = 'Q' then b.CHARGE_RATE*dcg.Container_Cnt 
    else b.CHARGE_RATE end as revenue_per_bill

, b.close_date 
, b.EFFECTIVE_DATE
, b.CHARGE_CODE
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
 WHERE b.CHARGE_TYPE = 'R' AND
    b.COMPANY_NUMBER in ('858','894','899','923','282', '320', '865')
	and  (b.CLOSE_DATE>20160701 or b.close_date = 0) 
	and 	b.EFFECTIVE_DATE<=20160701
	and b.current_ind='Y'
	and dcg.Acct_Type NOT in ('X','T') 
    AND dcg.Contract_Grp_Nbr = '0'  )
	select
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
