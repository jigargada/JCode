select top 1000 
dsc.Service_Cd
,dsc.Service_Cd_Desc
,dcg.Container_Cnt
,fsd.Service_Dt_SK
,dcg.container_Size
,fsd.is_deleted
,fsd.is_updated 
 from dwcore.dbo.fact_service_Detail fsd 
join dwcore.dbo.Dim_Container_Grp dcg
on fsd.container_grp_sk = dcg.Container_Grp_SK
join dwcore.dbo.Dim_Service_Cd dsc
on fsd.Service_Cd_SK = dsc.Service_Cd_SK

and dcg.acct_nbr = '1878' and dcg.infopro_div_nbr  = '026'


where Service_Dt_SK>20160000 and dsc.service_cd = 'ncc' 
order by fsd.Service_Dt_SK, dsc.Service_Cd


select * 

from ##inv_srvc 
where acct_nbr = '1878' and infopro_div_nbr  = '026'