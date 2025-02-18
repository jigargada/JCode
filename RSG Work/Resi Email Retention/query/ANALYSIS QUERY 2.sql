/****** Script for SelectTopNRows command from SSMS  ******/
SELECT M.[InfoProDivisionNumber]                 
,M.Account_Number
 ,     M.[Group]
 , case when m.group = 'A11' or m.group = 'A12' then '10% offer' 
       when m.group = 'A21' or m.group = 'A22' then  '4% offer' 
	   when m.group = 'HO' then 'Holdout' end as group_desc
	  ,M.DateSigned
	  ,M.revenue_Annual/12 as revenue_per_mo
	  ,ntile(5) over(partition by infopro_div_nbr order by case when total_yard_month = 0 then 0 else revenue_annual/total_yard_month end ) as revenue_group_start
	  ,ntile(5) over(partition by infopro_div_nbr order by case when total_yard_month = 0 then 0 else rate_per_mo_jul/total_yard_month end ) as revenue_group_jul
	  ,M.TENURE_MONTHS 
	  ,case when m.tenure_months<=18 then '18 mo or less'
	  when m.tenure_months>18  and tenure_months<=36 then '19 mo to 3 yrs'
	  when m.tenure_months>36  and tenure_months<=60 then '3 to 6 yrs'
	  when m.tenure_months>60  and tenure_months<=108 then '6 to 9 yrs'
	  when m.tenure_months>108  'more than 9 yrs' else 'err' end as tenure_grp
	  ,M.TOTAL_YARD_MONTH 
	  ,case when M.wave_1=1 or M.Wave_2=1 or M.wave_3=1 then 1 else 0 end as taker
	  ,case when M.datesigned is not null then 1 else 0 end  as taker_signed 
	  ,CASE WHEN R1.ACCT_NBR IS NULL THEN 0 ELSE 1 END AS ACTIVE_JUL
	  ,COALESCE(R1.RATE_PER_MO_TOT, 0 ) AS RATE_PER_MO_JUL
	  ,COALESCE(R1.REVENUE_AMT_INV, 0 ) AS REVENUE_AMT_JUL

	  ,CASE WHEN R2.ACCT_NBR IS NULL THEN 0 ELSE 1 END AS ACTIVE_AUG
	  ,COALESCE(R2.RATE_PER_MO_TOT, 0 ) AS RATE_PER_MO_AUG
	  ,COALESCE(R2.REVENUE_AMT_INV, 0 ) AS REVENUE_AMT_AUG

	  ,CASE WHEN R3.ACCT_NBR IS NULL THEN 0 ELSE 1 END AS ACTIVE_SEP
	  ,COALESCE(R3.RATE_PER_MO_TOT, 0 ) AS RATE_PER_MO_SEP
	  ,COALESCE(R3.REVENUE_AMT_INV, 0 ) AS REVENUE_AMT_SEP

	  ,CASE WHEN R4.ACCT_NBR IS NULL THEN 0 ELSE 1 END AS ACTIVE_OCT
	  ,COALESCE(R4.RATE_PER_MO_TOT, 0 ) AS RATE_PER_MO_OCT
	  ,COALESCE(R4.REVENUE_AMT_INV, 0 ) AS REVENUE_AMT_OCT

	  ,CASE WHEN R5.ACCT_NBR IS NULL THEN 0 ELSE 1 END AS ACTIVE_NOV
	  ,COALESCE(R5.RATE_PER_MO_TOT, 0 ) AS RATE_PER_MO_NOV
	  ,COALESCE(R5.REVENUE_AMT_INV, 0 ) AS REVENUE_AMT_NOV
      
	  ,COALESCE(R5.ACCOUNT_OPEN, R4.ACCOUNT_OPEN, R3.ACCOUNT_OPEN, R2.ACCOUNT_OPEN, R1.ACCOUNT_OPEN) AS ACCOUNT_OPEN_DATE
	  ,COALESCE(R5.ACCOUNT_CLOSE, R4.ACCOUNT_CLOSE, R3.ACCOUNT_CLOSE, R2.ACCOUNT_CLOSE, R1.ACCOUNT_CLOSE) AS ACCOUNT_CLOSE_DATE
	  
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