library(tidyverse)
library(lubridate)
library(stringr)
library(sparklyr)
library(data.table)
library(plyr)
library(dplyr)


in_path = '/global/project/queens-mma/scene2018/sample04/'

file_name = 'SceneAnalytics.dbo.LK_account_unique_member_identifier_sample10.csv'
LK_account_unique_member_identifier_sample10 = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_AccountBalance.csv'
SP_AccountBalance = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.SP_AccountHistory.csv'
SP_AccountHistory = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_AccountHistoryType.csv'
SP_AccountHistoryType = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_ActivityStatusScotiaScene_E.csv'
SP_ActivityStatusScotiaScene_E = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_CineplexStore.csv'
SP_CineplexStore = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_CustomerDetail.csv'
SP_CustomerDetail = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_CustomerExtension.csv'
SP_CustomerExtension = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_DimProxyLocation.csv'
SP_DimProxyLocation = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_FactAttribute.csv'
SP_FactAttribute = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_FactEnrollment.csv'
SP_FactEnrollment = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_LocationCARA.csv'
SP_LocationCARA = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_Location.csv'
SP_Location = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_Partner_E.csv'
SP_Partner_E = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_Points.csv'
SP_Points = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_PointsType.csv'
SP_PointsType = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_PointTypeStatistics.csv'
SP_PointTypeStatistics = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_ProxyPointTransaction_10.csv'
SP_ProxyPointTransaction_10 = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_QualityActivity.csv'
SP_QualityActivity = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)

file_name = 'SceneAnalytics.dbo.SP_Source.csv'
SP_Source = fread(paste(in_path, file_name, sep=""), sep=",", header=TRUE)


#Updated by Evert 2/5
# 1-updated to use sample04 data
# 2-recosystem at bottom now works 
# 3-RMSE added to each result to compare
# 4-checked for data quality. Looks good.. but simply too few people are active enough at Cara for good data
  
#--Universe Creation--#
#Create Universe 1, which essentially aggregates all the customer dimensions we care about (Account Balance, Enrollment metrics and Attributes)
universe1 <- SP_AccountBalance %>%
  inner_join(SP_FactEnrollment, by = "Unique_member_identifier") %>%
  select(Unique_member_identifier, Points, AccountCloseKey, isBNS, isVCL, isPrePaid) %>%
  inner_join(SP_FactAttribute, by = "Unique_member_identifier") %>%
  select(Unique_member_identifier, Points, AccountCloseKey, isBNS, isVCL, isPrePaid
         , isActive, OpensEmail_tendancy, RedemptionYearToDate, RedemptionProgramToDate)

#The latest month of activity
qual_last_mo <- SP_QualityActivity %>%
  summarize(max_date = max(ActivityMonth))

QualAct_new <- SP_QualityActivity %>%
  filter(ActivityMonth == as.integer(qual_last_mo))

#joined Quality Activity with universe 1 
universe2 <- universe1 %>%
  inner_join(QualAct_new, by = "Unique_member_identifier") %>%
  select(Unique_member_identifier, Points, AccountCloseKey, isBNS, isVCL
         , isPrePaid, isActive, OpensEmail_tendancy, RedemptionYearToDate, RedemptionProgramToDate
         , isQuality, isMarketable, isReachable, hasActivity)

#Universe 3
universe3 <- universe2 %>%
  #black card + email enrolled
  ##filter(isActive == TRUE) %>%
  #black card active
  filter(hasActivity == TRUE) %>%
  #keep only the UMI column
  select(Unique_member_identifier)
  #---Text Manipulation---#
  
  
  points1 = SP_Points %>% inner_join(universe3, by="Unique_member_identifier")%>%
    filter(ex_sourceid ==0 | pointtypeid ==1282,points>0) #Removing some points that seemed irrelevant
  
  points2 = points1 %>%
    mutate(ex_transactiondescription = gsub('\\\\d', '',ex_transactiondescription)) %>% # numbers
    mutate(ex_transactiondescription = gsub('[-#\'\\._/]', '',ex_transactiondescription)) %>% # special characters
    mutate(ex_transactiondescription = gsub('( AB | BC | MB| NB| NL| NS | NT | NU | ON | PE | QC | SK | YT)', '',ex_transactiondescription)) %>% #provinces
    mutate(ex_transactiondescription = gsub(' QTH', '',ex_transactiondescription)) %>% # Not sure what this is, but I saw it
    mutate(ex_transactiondescription = gsub('MCDONALDS Q', 'MCDONALDS',ex_transactiondescription)) %>%
    mutate(ex_transactiondescription = gsub('WENDYS QR', 'WENDYS',ex_transactiondescription)) %>%
    mutate(ex_transactiondescription = gsub('(TORONTO|VANCOUVER|OTTAWA|BRAMPTON|IRVING|LONDON|SCARBOROUGH|BURNABY|MISSISSAUGA)', '',ex_transactiondescription)) %>% # Big city names
    mutate(ex_transactiondescription = gsub('CGCTIM HORTONS', 'TIM HORTONS',ex_transactiondescription)) %>%
    mutate(ex_transactiondescription = gsub('TIM HORTONS QPS', 'TIM HORTONS',ex_transactiondescription)) %>%
    mutate(ex_transactiondescription = gsub('WAL-MART SUPERCENTER', 'WAL-MART',ex_transactiondescription)) %>%
    mutate(ex_transactiondescription = gsub(' +', ' ',ex_transactiondescription))%>% # Two or more spaces get turned into one
    mutate(ex_transactiondescription = gsub(' QPS', '',ex_transactiondescription)) %>% # Not sure what this
    mutate(ex_transactiondescription = gsub("SHOPPERS DRUG MART\\s\\d+|SHOPPERSDRUGMART.+|SHOPPERS DRUG MART.+", "SHOPPERS DRUG MART",ex_transactiondescription)) %>% # Not sure what this
    mutate(ex_transactiondescription = gsub("APL\\*\\sITUNESCOMBILL.+", "APL* ITUNESCOMBILL",ex_transactiondescription))%>% # Not sure what this
    mutate(ex_transactiondescription = gsub("AMAZON.+", "AMAZON",ex_transactiondescription)) %>% # Not sure what this
    mutate(ex_transactiondescription = gsub("VESTA\\s\\*CHATR.+", "VESTA *CHATR",ex_transactiondescription)) %>% # Not sure what this
    mutate(ex_transactiondescription = gsub('TIM HORTONS.+', 'TIM HORTONS',ex_transactiondescription)) %>% # Not sure what this
    mutate(ex_transactiondescription = gsub('NETFLIXCOM.+', 'NETFLIXCOM',ex_transactiondescription)) %>% # Not sure what this
    mutate(ex_transactiondescription = gsub('MCDONALDS.+|MCDONALD|MCDONALDS .+|MCDONALD .+', 'MCDONALDS',ex_transactiondescription))%>% # Not sure what this
    mutate(ex_transactiondescription = gsub('Amazon .+|Amazonca .+', 'AMAZON',ex_transactiondescription))%>% # Not sure what this
    mutate(ex_transactiondescription = gsub('UBER BV .+', 'UBER',ex_transactiondescription))%>% # Not sure what this
    mutate(ex_transactiondescription = gsub('UBER EATS .+', 'UBER EATS',ex_transactiondescription))%>% # Not sure what this
    mutate(ex_transactiondescription = gsub('REAL CDN .+ |REAL CANADIAN .+', 'REAL CANADIAN SUPERSTORE',ex_transactiondescription))%>% # Not sure what this
    mutate(ex_transactiondescription = gsub('GOOGLE .+', 'GOOGLE',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('WALMART\\s\\d+|WALMART\\s.+', 'WALMART',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('ATLANTIC SUPERSTORE .+', 'ATLANTIC SUPERSTORE',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('SOBEYS .+', 'SOBEYS',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('METRO .+', 'SOBEYS',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('PETROCAN.+', 'PETROCANADA',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('COSTCO .+', 'COSTCO',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('WENDYS .+', 'WENDYS',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('LONGOS .+', 'LONGOS',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('COMPASS .+', 'COMPASS-METRO',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('NO FRILLS .+|NOFRILLS .+', 'NO FRILLS',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('THE BEER STORE .+', 'THE BEER STORE',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('.+ CIRCLE K', 'CIRCLE K',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('.+ MACS .+', 'MACS CONVENIENCE',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('PIZZA PIZZA.+', 'PIZZA',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('STARBUCKS .+', 'STARBUCKS',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('SUBWAY .+', 'SUBWAY',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('LOBLAW .+', 'LOBLAW',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('ZEHRS .+', 'ZEHRS',ex_transactiondescription)) %>%# Not sure what this
    mutate(ex_transactiondescription = gsub('CANADIAN TIRE \\d+', 'CANADIAN TIRE',ex_transactiondescription))%>%# Not sure what this
    mutate(ex_transactiondescription = gsub('7ELEVEN \\d+', '7ELEVEN',ex_transactiondescription))%>%# Not sure what this
    mutate(ex_transactiondescription = gsub('PHARMA PLUS .+', 'PHARMA PLUS',ex_transactiondescription))%>%# Not sure what this
    mutate(ex_transactiondescription = gsub('PHARMAPRIX \\d+', 'PHARMAPRIX',ex_transactiondescription))%>%# Not sure what this
    mutate(ex_transactiondescription = gsub('FRESHCO \\d+', 'FRESHCO',ex_transactiondescription))%>%# Not sure what this
    mutate(ex_transactiondescription = gsub('LITTLE CAESARS PIZZA', 'LITTLE CAESARS',ex_transactiondescription))# Not sure what this
  
  detach("package:plyr", unload=TRUE) 
  
  # Creates a data table with only the top 150 merchants by transaction frequency
  points3 =points2%>%
    group_by(ex_transactiondescription) %>%
    summarise(n = n()) %>%
    arrange(desc(n))%>%
    slice(1:150) #grab the top 150 merchants
  
  #Modifies the merchant column so that CARA partner names show up 
  points2= points2 %>%
    inner_join(points3,by="ex_transactiondescription") %>%
    left_join(SP_DimProxyLocation[!duplicated(SP_DimProxyLocation$LocationCode),], by = c("ex_sourceid" = "LocationCode"))
  
  points2<-select(points2,Unique_member_identifier,points,ex_sourceid,pointdt,pointtypeid,ex_transactiondescription,BrandCode)
  points2<- mutate(points2,merchant=paste(points2$ex_transactiondescription,points2$BrandCode))
  points2<- mutate(points2,merchant = gsub(' NA', '',merchant))
  points2<- mutate(points2,merchant = gsub('CARA Points Earned ', '',merchant))
  
  #Filters out members with transactions between 100-200. This needs a bit of work to define what that transaction range should be. 
  points4<- points2%>%
    group_by(Unique_member_identifier)%>%
    summarise(n=n())%>%
    arrange(desc(n))%>%
    filter(n>=100,n<=200)
  #write_csv("points4.csv")
  
  #Add back merchants into the table
  points5 <- points4 %>% 
    inner_join(points2,by="Unique_member_identifier")%>%
    select(Unique_member_identifier,n,merchant,pointdt,ex_sourceid,points)%>%
    distinct(Unique_member_identifier,n,merchant,pointdt,ex_sourceid,points)%>%
    arrange(Unique_member_identifier)
  
  #write_csv("points4.csv")
  
  #Gets the number of transactions at each merchant for each customer
  points6 <- points5 %>%
    group_by(Unique_member_identifier,merchant)%>%
    arrange(Unique_member_identifier)%>%
    summarise(merch_n=n())
  
  group_number = (function(){i = 0; function() i <<- i+1 })()
  group_number2 = (function(){i = 0; function() i <<- i+1 })()
  
  #Builds the final ratings matrix
  ratings <- points5 %>%
    inner_join(points6, by="Unique_member_identifier")%>%
    mutate(rating=round(merch_n/n*10,digits=2))%>%
    #arranged by UMI to check something
    arrange(Unique_member_identifier)%>%
    distinct(Unique_member_identifier,n,merchant.y,pointdt,ex_sourceid,points,rating)%>%
    #select only the columns needed
    select(Unique_member_identifier,merchant.y,rating)%>%
    #created a new column with user ID's in descending order
    group_by(Unique_member_identifier) %>% 
    mutate(user = group_number())%>%
    #created a new column with brands in descending order
    group_by(merchant.y) %>% 
    mutate(item = group_number2())%>%
    distinct(Unique_member_identifier,n,merchant.y,rating)
  
#--t1 build--#

#Creating a data table w/ the following elements: Customers, Cara Partners, Frequency of 
# transactions by Customer to each Cara Partner, Total Transactions, and Scale (=Frequency of Transactions/Total Transactions)

t1 <- SP_Points %>%
  #only include chosen universe
  inner_join(universe3, by="Unique_member_identifier") %>%
  #deduplicated LocationCode, as it seemed like there were multiple locations associated with one location code
  #inner join Cara locations and earn transactions
  inner_join(SP_DimProxyLocation[!duplicated(SP_DimProxyLocation$LocationCode),], by = c("ex_sourceid" = "LocationCode")) %>%
  #select only the following columns: UMI, Points, ex_sourceid (ie. Location Code), BrandCode and date of transaction
  select(Unique_member_identifier, points, ex_sourceid, BrandCode,pointdt,pointtypeid)
  
  t2 <- t1 %>% 
  #filter for all point earning transaction (when points > 0)
  filter(points>0)%>%
  #filter for all CARA activity only
  filter(pointtypeid==1282)%>%
  #remove all duplicated values in the same day at the same time
  distinct(Unique_member_identifier, points, ex_sourceid, BrandCode,pointdt)%>%
  arrange(Unique_member_identifier)%>%
  na.omit() #63762 collectors left now

detach("package:plyr", unload=TRUE) 

#Create a new column with the total number of transactions by each customer, and new column with # of visis
t3 <- t2 %>%
  group_by(Unique_member_identifier)%>%
  arrange(Unique_member_identifier)%>%
  summarise(tot=n(),count=n_distinct(BrandCode))%>%
  filter(tot>2)%>%
  filter(count>2)%>%
  na.omit() %>%
  arrange(Unique_member_identifier)#7079 collectors left 

#distribution of all transactions
#Create a new column with the total number of transactions by each customer for each cara partner
t4 <- t2 %>%
  group_by(Unique_member_identifier,BrandCode)%>%
  arrange(Unique_member_identifier)%>%
  na.omit()%>%
  summarise(each=n())

#I believe we had to make the user ID's a descending value from 1 onwards... this is what the functions are for
group_number = (function(){i = 0; function() i <<- i+1 })()
group_number2 = (function(){i = 0; function() i <<- i+1 })()

#creates the ratings file
ratings <-inner_join(t4,t3,by="Unique_member_identifier")%>%
  #creates a column called "rating" which essentially gives us the preference a customer has for each retailer
  mutate(rating=round(each/tot*10,digits=2))%>%
  #arranged by UMI to check something
  arrange(Unique_member_identifier)%>%
  #select only the columns needed
  select(Unique_member_identifier,BrandCode,rating)%>%
  #created a new column with user ID's in descending order
  group_by(Unique_member_identifier) %>% 
  mutate(user = group_number())%>%
  #created a new column with brands in descending order
  group_by(BrandCode) %>% 
  mutate(item = group_number2())

#visualize_ratings(ratings_table = ratings)

#mutate(user = match(Unique_member_identifier, unique(Unique_member_identifier)),item = match(BrandCode, unique(BrandCode)))


#-----Start building recommender engine-----

#install.packages("recommenderlab")
#install.packages("recosystem")

library(devtools)
#install_github(repo = "SlopeOne", username = "tarashnot")
#install_github(repo = "SVDApproximation", username = "tarashnot")

library(recommenderlab)
library(recosystem)
library(SlopeOne)
library(SVDApproximation)

library(data.table)
library(RColorBrewer)
library(ggplot2)

sparse_ratings <- sparseMatrix(i = ratings$user, j = ratings$item, x = ratings$rating, 
                               dims = c(length(unique(ratings$user)), length(unique(ratings$item))),  
                               dimnames = list(paste("u", 1:length(unique(ratings$user)), sep = ""), 
                                               paste("m", 1:length(unique(ratings$item)), sep = "")))
#sparse_ratings[1:10, 1:10]

real_ratings <- new("realRatingMatrix", data = sparse_ratings)
real_ratings

#-----POPULAR MODEL-----#
model <- Recommender(real_ratings, method = "POPULAR", param=list(normalize = "center"))

prediction <- predict(model, real_ratings[1:9], type="ratings")
as(prediction, "matrix")[,1:9]
#max of [1:9]

set.seed(5)
e <- evaluationScheme(real_ratings, method="split", train=0.8, given=-1)
#5 ratings of 20% of users are excluded for testing, try with 70/30

model <- Recommender(getData(e, "train"), "POPULAR")
prediction <- predict(model, getData(e, "known"), type="ratings")

rmse_popular <- calcPredictionAccuracy(prediction, getData(e, "unknown"))[1]
rmse_popular #Popular-2.56




#-----CF-USER BASED-----#

model <- Recommender(real_ratings, method = "UBCF", 
                     param=list(normalize = "center", method="Cosine", nn=50))

model <- Recommender(real_ratings, method = "UBCF", 
                     param=list(normalize = "center", method="Jaccard", nn=50))

model <- Recommender(real_ratings, method = "UBCF", 
                     param=list(normalize = "center", method="Pearson", nn=50))

prediction <- predict(model, real_ratings[1:10], type="ratings")
as(prediction, "matrix")[,1:9]

#Estimating RMSE

#model <- Recommender(getData(e, "train"), method = "UBCF", 
#                     param=list(normalize = "center", method="Cosine", nn=50))

set.seed(1)
prediction <- predict(model, getData(e, "known"), type="ratings")

rmse_ubcf <- calcPredictionAccuracy(prediction, getData(e, "unknown"))[1]
rmse_ubcf 
#Cosine-2.66. Jaccard-2.644. Pearson-2.578








#-----RECOSYSTEM-----#
ratings_example <- data(ratings)

ratings_reco <- ratings %>%
  arrange(item) %>%
  ungroup() %>%
  select(user, item, rating)

ratings_reco <- as.data.table(ratings_reco)

set.seed(1)
in_train <- rep(TRUE, nrow(ratings_reco))
in_train[sample(1:nrow(ratings_reco), size = round(0.2 * length(unique(ratings_reco$user)), 0) * 5)] <- FALSE

ratings_train <- ratings_reco[(in_train)]
ratings_test <- ratings_reco[(!in_train)]

write.table(ratings_train, file = "trainset.txt", sep = " ", row.names = FALSE, col.names = FALSE)
write.table(ratings_test, file = "testset.txt", sep = " ", row.names = FALSE, col.names = FALSE)

r = Reco()

#this option runs too slowly
#opts <- r$tune(data_file("trainset.txt"), opts = list(dim = c(1:20), lrate = c(0.05),
#                                                      nthread = 4, costp_l1 = c(0, 0.1),costp_l2 = c(0.01, 0.1),
#                                                      costq_l1 = c(0, 0.1), niter = 200, nfold = 10, verbose = FALSE))

opts <- r$tune(data_file("trainset.txt"), opts = list(dim      = c(10L, 20L),
                                                      costp_l1 = c(0, 0.1),
                                                      costp_l2 = c(0.01, 0.1),
                                                      costq_l1 = c(0, 0.1),
                                                      costq_l2 = c(0.01, 0.1),
                                                      lrate    = c(0.01, 0.1))
)

r$train(data_file("trainset.txt"), opts = c(opts$min, nthread = 4, niter = 500, verbose = FALSE))

outfile = tempfile()

r$predict(data_file("trainset.txt"), out_file("predict.txt"))

scores_real <- read.table("testset.txt", header = FALSE, sep = " ")$V3
scores_pred <- scan(outfile)

rmse_mf <- sqrt(mean((scores_real-scores_pred) ^ 2))

rmse_mf #2.43


