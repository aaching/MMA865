library(tidyverse)
library(lubridate)
library(stringr)
library(sparklyr)
library(data.table)
library(dplyr)
sc<-spark_connect(master='yarn-client')
hdfs_path = '/user/hpc3552/scene2018/sample01/'
file_name = 'SceneAnalytics.dbo.SP_Points.csv'
points = spark_read_csv(sc, name='sp_points',  path=paste(hdfs_path, file_name, sep=""),
                        header = TRUE, delimiter = ",")
file_name = 'SceneAnalytics.dbo.SP_FactAttribute.csv'
factattr = spark_read_csv(sc, name='factattr',  path=paste(hdfs_path, file_name, sep=""),
                          header = TRUE, delimiter = ",")
file_name = 'SceneAnalytics.dbo.SP_FactEnrollment.csv'
factenroll = spark_read_csv(sc, name='factenroll',  path=paste(hdfs_path, file_name, sep=""),
                            header = TRUE, delimiter = ",")
file_name = 'SceneAnalytics.dbo.SP_CustomerDetail.csv'
custdetail = spark_read_csv(sc, name='custdetail',  path=paste(hdfs_path, file_name, sep=""),
                            header = TRUE, delimiter = ",")
file_name = 'SceneAnalytics.dbo.SP_DimProxyLocation.csv'
proxyloc = spark_read_csv(sc, name='proxyloc',  path=paste(hdfs_path, file_name, sep=""),
                          header = TRUE, delimiter = ",")
proxyloc=rename(proxyloc,ex_sourceid=LocationCode)
#colnames(proxyloc)[colnames(proxyloc)=="LocationCode"] <- "ex_sourceid"
proxyloc
universal_data = inner_join(factattr,factenroll,by="Unique_member_identifier")
universal_data = filter(universal_data,OpensEmail_tendancy=="TRUE", isBNS=="TRUE" | isVCL=="TRUE")
universal_data = left_join(universal_data,custdetail,by="Unique_member_identifier")
universal_data = left_join(universal_data,points,by="Unique_member_identifier")
universal_data = left_join(universal_data,proxyloc,by="ex_sourceid")
universal_data
