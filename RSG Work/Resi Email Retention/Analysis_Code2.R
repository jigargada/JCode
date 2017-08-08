

library(data.table)
library(xlsx)
require(readxl)
library(stringr)
library(rsgutilr)

setwd("//devpricapp01/d/Jigar/Pricing-Analytics/projects/Resi Email Retention/data_in")

filenames <- list.files(pattern=".xlsx")

table_Open <- list()
table_click <- list()
table_did_not_open <- list()
table_did_not_click <- list()
table_opt_out <- list()

for(i in 1:length(filenames)) 
{
 
  table_Open[[i]] <- data.table(read_excel(filenames[i], sheet = 2))
  table_Open[[i]] <- table_Open[[i]][,':='(Open = '1', Wave = substr(filenames[i], 23, 24),
                                           Wave_Date = str_extract(filenames[i], "[[:digit:]]{1,2}\\.[[:digit:]]{1,2}\\.[[:digit:]]{1,2}"))]
  
  table_click[[i]] <- data.table(read_excel(filenames[i], sheet = 3))
  table_click[[i]] <- table_click[[i]][,':='(Click = '1', Wave = substr(filenames[i], 23, 24),
                                            Wave_Date = str_extract(filenames[1], "[[:digit:]]{1,2}\\.[[:digit:]]{1,2}\\.[[:digit:]]{1,2}"))]
  
  table_did_not_open[[i]] <- data.table(read_excel(filenames[i], sheet = 4))
  table_did_not_open[[i]] <- table_did_not_open[[i]][,':='(Did_not_open = '1', Wave = substr(filenames[i], 23, 24),
                                            Wave_Date = str_extract(filenames[1], "[[:digit:]]{1,2}\\.[[:digit:]]{1,2}\\.[[:digit:]]{1,2}"))]
  
  table_did_not_click[[i]] <- data.table(read_excel(filenames[i], sheet = 5))
  table_did_not_click[[i]] <- table_did_not_click[[i]][,':='(Did_not_click = '1', Wave = substr(filenames[i], 23, 24),
                                            Wave_Date= str_extract(filenames[1], "[[:digit:]]{1,2}\\.[[:digit:]]{1,2}\\.[[:digit:]]{1,2}"))]
  
  table_opt_out[[i]] <- data.table(read_excel(filenames[i], sheet = 6))
  table_opt_out[[i]] <- table_opt_out[[i]][,':='(Opt_out = '1', Wave = substr(filenames[i], 23, 24),
                                                 Wave_Date= str_extract(filenames[1], "[[:digit:]]{1,2}\\.[[:digit:]]{1,2}\\.[[:digit:]]{1,2}"))]
  
  
}

for(i in 1:length(filenames)) 
{
  table_Open[[i]] <- table_Open[[i]][, list(Account, Open, Wave, Wave_Date, Recycle, SolidWaste)]
  table_click[[i]] <- table_click[[i]][, list(Account, Click, Wave, Wave_Date, Recycle, SolidWaste)]
  table_did_not_open[[i]] <- table_did_not_open[[i]][, list(Account, Did_not_open, Wave, Wave_Date, Recycle, SolidWaste)]
  table_did_not_click[[i]] <- table_did_not_click[[i]][, list(Account, Did_not_click, Wave, Wave_Date, Recycle, SolidWaste)]
  table_opt_out[[i]] <- table_opt_out[[i]][, list(Account, Opt_out, Wave, Wave_Date, Recycle, SolidWaste)]
}  

for(i in 1:length(filenames)) 
{
  if (i==1)
  {
    Opened <- table_Open[[1]] 
    Clicked <- table_click[[1]] 
    did_not_open <- table_did_not_open[[1]] 
    did_not_click <- table_did_not_click[[1]] 
    opt_out <- table_opt_out[[1]]
  }  
  else
  {
     Opened <- rbind(Opened, table_Open[[i]])
     Clicked <- rbind(Clicked, table_click[[i]])
     did_not_open <- rbind(did_not_open, table_did_not_open[[i]]) 
     did_not_click <- rbind(did_not_click, table_did_not_click[[i]])
     opt_out <- rbind(opt_out, table_opt_out[[i]])
  }
}

Opened <- Opened[!is.na(Account)]
Opened <- Opened[, ':='(Lawson = substr(Account, 1,4))]
Clicked <- Clicked[!is.na(Account)]
Clicked <- Clicked[, ':='(Lawson = substr(Account, 1,4))]
did_not_open <- did_not_open[!is.na(Account)]
did_not_open <- did_not_open[, ':='(Lawson = substr(Account, 1,4))]
did_not_click <- did_not_click[!is.na(Account)]
did_not_click <- did_not_click[, ':='(Lawson = substr(Account, 1,4))]
opt_out <- opt_out[!is.na(Account)]
opt_out <- opt_out[, ':='(Lawson = substr(Account, 1,4))]


database_tables <- data.table(sqlProc(server="prodbirpt", db="DWCORE",
                      sqlFile="SELECT dch.Cur_Div_Nbr
                      ,dcg.[Infopro_Div_Nbr]
                      ,dcg.[Acct_Nbr]
                      ,CONCAT(dch.[Cur_Div_Nbr] ,dcg.[Acct_Nbr]) as Lawson_Acct
                      FROM DWCORE.dbo.Dim_Container_Grp dcg
                      Inner join [DWCORE].[dbo].[Dim_Corp_Hier] dch
                      on dch.Rev_Distrib_Cd = dcg.Revenue_Distribution_Cd and dch.Infopro_Div_Nbr = dcg.Infopro_Div_Nbr
                      and dcg.is_Current = 1 and dch.is_Current = 1 and dcg.Infopro_Div_Nbr in 
                      ('858', '894', '899', '923', '865', '282', '320', '324') and dch.Infopro_Div_Nbr in 
                      ('858', '894', '899', '923', '865', '282', '320', '324') and dch.Cur_Div_Nbr in 
                      ('3211', '3282', '4320', '3508', '4318', '4324', '3056', '3250', '3421', '3820', '3832', '4094', 
                      '4095', '4176', '4324', '4753', '4858', '4894', '4899', '4923', '4930', '4972')",
                      asText=TRUE,windows_auth = TRUE, as.is = TRUE))

database_tables <- database_tables[nchar(lawson_acct) <11, ':='(lawson_acct = paste0(cur_div_nbr, 
                                                                                     sprintf("%07.f", as.numeric(acct_nbr))))]


setkey(Opened, Account)
setkey(database_tables, lawson_acct)

Opened_joined <- merge(Opened, database_tables, by.x= "Account",
                          by.y= "lawson_acct", all.x = TRUE)

Opened_joined <- Opened_joined[, ':='(Acct_new = ifelse(is.na(infopro_div_nbr),Account, paste0(infopro_div_nbr, '-', 
                                                        sprintf("%07.f", as.numeric(acct_nbr)))))]

Opened_joined <- Opened_joined[,':='(cur_div_nbr = NULL, infopro_div_nbr = NULL,
                                     acct_nbr =NULL, Lawson = NULL, Account = NULL, Click = '', Did_not_open = '',
                                     Did_not_click = '', Opt_out = '')]
setcolorder(Opened_joined, c("Acct_new", "Wave", "Wave_Date", "Recycle", "SolidWaste",
            "Open", "Click", "Did_not_open", "Did_not_click", "Opt_out"))



setkey(Clicked, Account)
setkey(database_tables, lawson_acct)

Clicked_joined <- merge(Clicked, database_tables, by.x= "Account",
                       by.y= "lawson_acct", all.x = TRUE)

Clicked_joined <- Clicked_joined[, ':='(Acct_new = ifelse(is.na(infopro_div_nbr),Account, paste0(infopro_div_nbr, '-', 
                                                                                               sprintf("%07.f", as.numeric(acct_nbr)))))]

Clicked_joined <- Clicked_joined[,':='(cur_div_nbr = NULL, infopro_div_nbr = NULL,
                                     acct_nbr =NULL, Lawson = NULL, Account = NULL, Open = '', Did_not_open = '',
                                     Did_not_click = '', Opt_out = '')]
setcolorder(Clicked_joined, c("Acct_new", "Wave", "Wave_Date", "Recycle", "SolidWaste",
            "Open", "Click", "Did_not_open", "Did_not_click", "Opt_out"))


setkey(did_not_open, Account)
setkey(database_tables, lawson_acct)

did_not_open_joined <- merge(did_not_open, database_tables, by.x= "Account",
                        by.y= "lawson_acct", all.x = TRUE)

did_not_open_joined <- did_not_open_joined[, ':='(Acct_new = ifelse(is.na(infopro_div_nbr),Account, paste0(infopro_div_nbr, '-', 
                                                                                                 sprintf("%07.f", as.numeric(acct_nbr)))))]

did_not_open_joined <- did_not_open_joined[,':='(cur_div_nbr = NULL, infopro_div_nbr = NULL,
                                       acct_nbr =NULL, Lawson = NULL, Account = NULL, Open = '', Click = '',
                                       Did_not_click = '', Opt_out = '')]
setcolorder(did_not_open_joined, c("Acct_new", "Wave", "Wave_Date", "Recycle", "SolidWaste",
            "Open", "Click", "Did_not_open", "Did_not_click", "Opt_out"))


setkey(did_not_click, Account)
setkey(database_tables, lawson_acct)

did_not_click_joined <- merge(did_not_click, database_tables, by.x= "Account",
                             by.y= "lawson_acct", all.x = TRUE)

did_not_click_joined <- did_not_click_joined[, ':='(Acct_new = ifelse(is.na(infopro_div_nbr),Account, paste0(infopro_div_nbr, '-', 
                                                                                                           sprintf("%07.f", as.numeric(acct_nbr)))))]

did_not_click_joined <- did_not_click_joined[,':='(cur_div_nbr = NULL, infopro_div_nbr = NULL,
                                       acct_nbr =NULL, Lawson = NULL, Account = NULL, Open = '', Click = '',
                                       Did_not_open = '', Opt_out = '')]
setcolorder(did_not_click_joined, c("Acct_new", "Wave", "Wave_Date", "Recycle", "SolidWaste",
            "Open", "Click", "Did_not_open", "Did_not_click", "Opt_out"))



setkey(opt_out, Account)
setkey(database_tables, lawson_acct)

opt_out_joined <- merge(opt_out, database_tables, by.x= "Account",
                              by.y= "lawson_acct", all.x = TRUE)

opt_out_joined <- opt_out_joined[, ':='(Acct_new = ifelse(is.na(infopro_div_nbr),Account, paste0(infopro_div_nbr, '-', 
                                                                                                             sprintf("%07.f", as.numeric(acct_nbr)))))]

opt_out_joined <- opt_out_joined[,':='(cur_div_nbr = NULL, infopro_div_nbr = NULL,
                                                   acct_nbr =NULL, Lawson = NULL, Account = NULL, Open = '', Click = '',
                                                   Did_not_open = '', Did_not_click = '')]

setcolorder(opt_out_joined, c("Acct_new", "Wave", "Wave_Date", "Recycle", "SolidWaste",
            "Open", "Click", "Did_not_open", "Did_not_click", "Opt_out"))


consolidated <- rbind(Opened_joined, Clicked_joined, did_not_click_joined, did_not_open_joined, opt_out_joined)

consolidated <- unique(consolidated)

consolidated <- consolidated[, ":="(Recycle = ifelse(Recycle == 'Y', 1, 0),
                                    SolidWaste = ifelse(SolidWaste == 'Y', 1, 0))]

consolidated <- consolidated[, ":="(Open = ifelse(Did_not_open == 1, 0, Open),
                                    Click= ifelse(Did_not_click == 1, 0, Click))]

consolidated_1 <- consolidated [, ":="(Wave=NULL, Wave_Date=NULL, Did_not_open = NULL, Did_not_click = NULL)]

consolidated_uniq <- consolidated_1[, lapply(.SD, max), .SDcols = c("Open", "Click"),  by = c("Acct_new", "Recycle", "SolidWaste", "Opt_out")]

write.table(consolidated_uniq, paste0("email_ret_Open_click.csv"), sep=",",na="",row.names=FALSE)








consolidated_uniq <- unique(consolidated_1)

table_Open[[1]] <- table_Open[[1]][,':='(Open = '1', Wave = substr(filenames[i], 23, 24),
                                         Date = substr(filenames[i], length(filenames[i])-8, length(filenames[i])))]


#table1 <- read.xlsx("Retention Email Touch 1 - Little Rock (BU233) 7.21.16.xlsx", 2)
#rbindlist(filenames, lapply(filenames, function(x) read.xlsx(file=x, sheetIndex=2, header=TRUE, FILENAMEVAR=x)))
