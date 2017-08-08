drop table ##temp_1
go
Select ma.InfoProDivisionNumber 
, ma.Account_Number
, ma.ServiceZip
, ma.revenue_per_bill
, ma.revenue_annual
, ma.future_rate
, case when ma.future_rate != 0 then 1 else 0 end as [Future Revenue Flag]
, ma.Year_1	
, ma.Year_2
, ma.Year_3
, ma.tenure_months	
, case when ma.tenure_months<36 then 'Less than 3 Years'
		when ma.Tenure_months between 37 and 72	then 'Between 3 and 6 Years'
		when ma.Tenure_months between 73 and 120	then 'Between 6 and 10 Years'
		when ma.Tenure_months between 121 and 180 then 'Between 10 and 15 Years'
Else 'Over 15 Years' End as [Tenure Range]
, ma.Profitability	
--, case when ma.profitability<0.55 then "Less than 0.55"
--		when ma.profitability >= 0.55 and ma.profitability <0.75 then "Between 0.55 and 0.75"
--		when ma.profitability >= 0.75 and ma.profitability <0.9 then "Between 0.75 and 0.9"
--		when ma.profitability >= 0.9 and ma.profitability <1.25 then "Between 0.9 and 1.25"
--		when ma.profitability >= 1.25 and ma.profitability <2.0 then "Between 1.25 and 2.0"
--		Else  ma.profitability >= 2.0 End as [Profitability Range]
, ma.Cat_Rank 
, ma.INV
, ma.EVR
, ma.FRF
, ma.DateSigned	
, ma.container_cnt	
, ma.Total_yard_month
, ma.RCY
, ma.SW	
, ma.account_open_date	
, ma.[Group]
, left(ma.[Group], 2 ) as group2
, ma.Wave_1
, ma.Wave_2	
, ma.Wave_3	
, cast(case when month_dt = 20160701 then 1 else 0 end as bit) as July_Status
, case when month_dt = 20160701 then rate_per_mo_base else 0 end as July_revenue						
 into ##temp_1
from 
[Marketing_Campaign].[dbo].[Mkt_Analysis_1] ma Left Join [Beckie].[dbo].[residential_new] ro
on ma.infoprodivisionnumber= ro.Infopro_div_nbr and  ma.Account_Number = ro.acct_nbr and month_dt = 20160701



drop table ##temp_2
go
Select ma.*
, cast(case when month_dt = 20160801 then 1 else 0 end as bit) as August_Status	
, case when month_dt = 20160801 then rate_per_mo_base else 0 end as August_Rate
into ##temp_2
 from ##temp_1 ma Left Join [Beckie].[dbo].[residential_new] ro
on ma.infoprodivisionnumber= ro.Infopro_div_nbr and  ma.Account_Number = ro.acct_nbr and month_dt = 20160801

 
 drop table ##temp_3
go
Select ma.*
, cast(case when month_dt = 20160901 then 1 else 0 end as bit) as Sept_Status	
, case when month_dt = 20160901 then rate_per_mo_base else 0 end as Sept_Rate
into ##temp_3
 from ##temp_2 ma Left Join [Beckie].[dbo].[residential_new] ro
on ma.infoprodivisionnumber= ro.Infopro_div_nbr and  ma.Account_Number = ro.acct_nbr and month_dt = 20160901


drop table ##temp_4
go
Select ma.*
, cast(case when month_dt = 20161001 then 1 else 0 end as bit) as Oct_Status	
, case when month_dt = 20161001 then rate_per_mo_base else 0 end as Oct_Rate
into ##temp_4
 from ##temp_3 ma Left Join [Beckie].[dbo].[residential_new] ro
on ma.infoprodivisionnumber= ro.Infopro_div_nbr and  ma.Account_Number = ro.acct_nbr and month_dt = 20161001


drop table ##temp_5
go
Select ma.*
, cast(case when month_dt = 20161101 then 1 else 0 end as bit) as Nov_Status	
, case when month_dt = 20161101 then rate_per_mo_base else 0 end as Nov_Rate
into ##temp_5
 from ##temp_4 ma Left Join [Beckie].[dbo].[residential_new] ro
on ma.infoprodivisionnumber= ro.Infopro_div_nbr and  ma.Account_Number = ro.acct_nbr and month_dt = 20161101


drop table ##temp_6
go
Select ma.*
, cast(case when month_dt = 20161201 then 1 else 0 end as bit) as Dec_Status	
, case when month_dt = 20161201 then rate_per_mo_base else 0 end as Dec_Rate
into ##temp_6
 from ##temp_5 ma Left Join [Beckie].[dbo].[residential_update] ro
on ma.infoprodivisionnumber= ro.Infopro_div_nbr and  ma.Account_Number = ro.acct_nbr and month_dt = 20161201



Select * from ##temp_6