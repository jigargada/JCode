library(data.table)
library(RODBC)
library(xlsx)
library(stringr)
library(rsgutilr)

mkt <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Email Retention Cust_Data (for Marketing dept)_V4.csv", header = TRUE))

success = data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Upload Success.csv", header=TRUE))

takers_mn <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 1 - July 2016_MN.csv", header=TRUE))
takers_nj <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 1 - July 2016_NJ.csv", header=TRUE))
takers_littlerock <- data.table(read.csv("//prodpricapp02/d/Analytics/Resi Email Retention/Executed Contracts - Touch 1 - July 2016_Little_Rock.csv", header=TRUE))

takers<- rbind(takers_mn, takers_nj, takers_littlerock)
