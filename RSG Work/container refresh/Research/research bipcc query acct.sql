 declare @month as datetime,   @monthint as int, @monthcl as datetime, @monthintend as int
set @month = '2016-02-01 00:00:00.000' 
set @monthcl = dateadd(month, 1, @month)
set @monthint= year(@month)*10000+month(@month)*100+1
set @monthintend=year(@monthcl)*10000+month(@monthcl)*100+1; 

select * from dwpsa.dbo.STG_IFP_BIPCC_2 r 
where cccomp  = '026' and ccacct = '1005719' 
   and r.CCCHCD in ('BCR', 'BSP', 'BCE', 'BCS')
  
  and [CCEFDT] < @monthintend  
  and ([CCCLDT] > @monthint or  [CCCLDT] = 0 )  
  and (r.cccldt>= r.ccefdt or r.cccldt=0)    
  and R.current_ind = 'y' 
   and R.CCBADJ= '0' 
     	   

go
 declare @month as datetime,   @monthint as int, @monthcl as datetime, @monthintend as int
set @month = '2016-03-01 00:00:00.000' 
set @monthcl = dateadd(month, 1, @month)
set @monthint= year(@month)*10000+month(@month)*100+1
set @monthintend=year(@monthcl)*10000+month(@monthcl)*100+1; 

select * from dwpsa.dbo.STG_IFP_BIPCC_2 r 
where cccomp  = '026' and ccacct = '1005719' 
   and r.CCCHCD in ('BCR', 'BSP', 'BCE', 'BCS')
  
  and [CCEFDT] < @monthintend
  
  and ([CCCLDT] > @monthint or  [CCCLDT] = 0 )
  
  and (r.cccldt>= r.ccefdt or r.cccldt=0)    
  and R.current_ind = 'y' 
   and R.CCBADJ= '0' 
     	   
