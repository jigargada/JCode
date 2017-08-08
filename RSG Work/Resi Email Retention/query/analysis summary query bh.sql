/****** Script for SelectTopNRows command from SSMS  ******/
with dat1 as (
SELECT M.[InfoProDivisionNumber]                 
 , case when m.[group] = 'A21' or m.[group] = 'A22' then '10% offer' 
       when m.[group] = 'A11' or m.[group] = 'A12' then  '4% offer' 
	   when m.[group] = 'HO' then 'Holdout' else 'err' end as group_desc

	   ,ntile(5) over(partition by m.infoprodivisionnumber order by case when total_yard_month = 0 then 0 else revenue_annual/total_yard_month end ) as revenue_group_start
	  ,ntile(5) over(partition by m.infoprodivisionnumber order by case when total_yard_month = 0 then 0 else COALESCE(R1.RATE_PER_MO_TOT, 0 )/total_yard_month end ) as revenue_group_jul
	  ,case when m.tenure_months<=18 then '18 mo or less'
	  when m.tenure_months>18  and tenure_months<=36 then '19 mo to 3 yrs'
	  when m.tenure_months>36  and tenure_months<=60 then '3 to 6 yrs'
	  when m.tenure_months>60  and tenure_months<=108 then '6 to 9 yrs'
	  when m.tenure_months>108  then 'more than 9 yrs' else 'err' end as tenure_grp
	  ,case when M.wave_1=1 or M.Wave_2=1 or M.wave_3=1 then 1 else 0 end as taker
	  ,case when M.datesigned is not null then 1 else 0 end  as taker_signed 
	  ,case when m.future_revenue_per_bill is not null then 'Future Rate' else 'No Future Rate' end as future_rate
	  ,case when m.future_revenue_per_bill > m.revenue_per_bill then 1 else 0 end as future_rate_higher
	  	  ,M.revenue_Annual/12 as revenue_per_mo_start	  
	  ,M.TENURE_MONTHS  
	  ,M.TOTAL_YARD_MONTH 
	  
	  ,CASE WHEN R1.ACCT_NBR IS NULL THEN 0 ELSE 1 END AS ACTIVE_JUL
	  ,COALESCE(R1.RATE_PER_MO_TOT, 0 ) AS RATE_PER_MO_JUL
	  ,COALESCE(R1.REVENUE_AMT_INV, 0 ) AS REVENUE_AMT_JUL

	  ,CASE WHEN R2.ACCT_NBR IS NULL THEN 0 ELSE 1 END AS ACTIVE_AUG
	  ,COALESCE(R2.RATE_PER_MO_TOT, 0) AS RATE_PER_MO_AUG
	  ,COALESCE(R2.REVENUE_AMT_INV, 0) AS REVENUE_AMT_AUG

	  ,CASE WHEN R3.ACCT_NBR IS NULL THEN 0 ELSE 1 END AS ACTIVE_SEP
	  ,COALESCE(R3.RATE_PER_MO_TOT, 0 ) AS RATE_PER_MO_SEP
	  ,COALESCE(R3.REVENUE_AMT_INV, 0 ) AS REVENUE_AMT_SEP

	  ,CASE WHEN R4.ACCT_NBR IS NULL THEN 0 ELSE 1 END AS ACTIVE_OCT
	  ,COALESCE(R4.RATE_PER_MO_TOT, 0 ) AS RATE_PER_MO_OCT
	  ,COALESCE(R4.REVENUE_AMT_INV, 0 ) AS REVENUE_AMT_OCT

	  ,CASE WHEN R5.ACCT_NBR IS NULL THEN 0 ELSE 1 END AS ACTIVE_NOV
	  ,COALESCE(R5.RATE_PER_MO_TOT, 0 ) AS RATE_PER_MO_NOV
	  ,COALESCE(R5.REVENUE_AMT_INV, 0 ) AS REVENUE_AMT_NOV
      
	  ,CASE WHEN R6.ACCT_NBR IS NULL THEN 0 ELSE 1 END AS ACTIVE_DEC
	  ,COALESCE(R6.RATE_PER_MO_TOT, 0 ) AS RATE_PER_MO_DEC
	  ,COALESCE(R6.REVENUE_AMT_INV, 0 ) AS REVENUE_AMT_DEC

	  ,CASE WHEN R7.ACCT_NBR IS NULL THEN 0 ELSE 1 END AS ACTIVE_JAN
	  ,COALESCE(R7.RATE_PER_MO_TOT, 0 ) AS RATE_PER_MO_JAN
	  ,COALESCE(R7.REVENUE_AMT_INV, 0 ) AS REVENUE_AMT_JAN

	  
  FROM [Marketing_Campaign].[dbo].[Mkt_Analysis] M
  LEFT JOIN BECKIE.DBO.RESIDENTIAL_NEW R1 
  ON M.InfoProDivisionNumber = R1.INFOPRO_DIV_NBR
  AND M.Account_Number = R1.ACCT_NBR 
  AND R1.MONTH_DT = 20160701
  
LEFT JOIN BECKIE.DBO.RESIDENTIAL_NEW R2 
  ON M.InfoProDivisionNumber = R2.INFOPRO_DIV_NBR
  AND M.Account_Number = R2.ACCT_NBR 
  AND R2.MONTH_DT = 20160801


LEFT JOIN BECKIE.DBO.RESIDENTIAL_NEW R3
  ON M.InfoProDivisionNumber = R3.INFOPRO_DIV_NBR
  AND M.Account_Number = R3.ACCT_NBR 
  AND R3.MONTH_DT = 20160901


LEFT JOIN BECKIE.DBO.RESIDENTIAL_NEW R4 
  ON M.InfoProDivisionNumber = R4.INFOPRO_DIV_NBR
  AND M.Account_Number = R4.ACCT_NBR 
  AND R4.MONTH_DT = 20161001

LEFT JOIN BECKIE.DBO.RESIDENTIAL_NEW R5 
  ON M.InfoProDivisionNumber = R5.INFOPRO_DIV_NBR
  AND M.Account_Number = R5.ACCT_NBR 
  AND R5.MONTH_DT = 20161101	

  LEFT JOIN BECKIE.DBO.RESIDENTIAL_NEW R6
  ON M.InfoProDivisionNumber = R6.INFOPRO_DIV_NBR
  AND M.Account_Number = R6.ACCT_NBR 
  AND R6.MONTH_DT = 20161201	

  LEFT JOIN BECKIE.DBO.RESIDENTIAL_NEW R7
  ON M.InfoProDivisionNumber = R7.INFOPRO_DIV_NBR
  AND M.Account_Number = R7.ACCT_NBR 
  AND R7.MONTH_DT = 20170101	

    )
	select 
	InfoProDivisionNumber
 ,  group_desc
	   ,revenue_group_start
	  ,revenue_group_jul
	  ,tenure_grp
	  ,taker
	  ,taker_signed 
	  ,future_rate
	  ,future_rate_higher	
	,count(*) as accts_start
	,sum(case when taker+taker_signed>0 then 1 else 0 end) as num_takers
	  ,sum(revenue_per_mo_start	  ) as revenue_per_mo_start	  
	  ,sum(TENURE_MONTHS ) as total_tenure_mos	  
	  ,sum(TOTAL_YARD_MONTH ) as total_yard_month	  
	  
	  ,sum(ACTIVE_JUL) as Active_Jul
	  ,sum(RATE_PER_MO_JUL) as Rate_Per_Mo_Jul
	  ,sum(REVENUE_AMT_JUL) as Rev_Amt_Jul

	  ,sum(ACTIVE_Aug) as Active_Aug
	  ,sum(RATE_PER_MO_Aug) as Rate_Per_Mo_Aug
	  ,sum(REVENUE_AMT_Aug) as Rev_Amt_Aug
	
	 ,sum(ACTIVE_Sep) as Active_Sep
	  ,sum(RATE_PER_MO_Sep) as Rate_Per_Mo_Sep
	  ,sum(REVENUE_AMT_Sep) as Rev_Amt_Sep

	,sum(ACTIVE_Oct) as Active_Oct
	  ,sum(RATE_PER_MO_Oct) as Rate_Per_Mo_Oct
	  ,sum(REVENUE_AMT_Oct) as Rev_Amt_Oct

,sum(ACTIVE_Nov) as Active_Nov
	  ,sum(RATE_PER_MO_Nov) as Rate_Per_Mo_Nov
	  ,sum(REVENUE_AMT_Nov) as Rev_Amt_Nov

,sum(ACTIVE_Dec) as Active_Dec
	  ,sum(RATE_PER_MO_Dec) as Rate_Per_Mo_Dec
	  ,sum(REVENUE_AMT_Dec) as Rev_Amt_Dec

,sum(ACTIVE_Jan) as Active_Jan
	  ,sum(RATE_PER_MO_Jan) as Rate_Per_Mo_Jan
	  ,sum(REVENUE_AMT_Jan) as Rev_Amt_Jan


    from dat1  
	group by InfoProDivisionNumber
    ,  group_desc
	   ,revenue_group_start
	  ,revenue_group_jul
	  ,tenure_grp
	  ,taker
	  ,taker_signed 	
	  ,future_rate
	  ,future_rate_higher	