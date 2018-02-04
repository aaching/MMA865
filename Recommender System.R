library(tidyverse)
library(lubridate)
library(stringr)
library(sparklyr)
library(data.table)
library(plyr)
library(dplyr)


in_path = '/global/project/queens-mma/scene2018/sample02/'

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


#create universe

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
  filter(isActive == TRUE) %>%
  #black card active
  filter(hasActivity == TRUE) %>%
  #keep only the UMI column
  select(Unique_member_identifier)


#Creating a data table w/ the following elements: Customers, Cara Partners, Frequency of 
# transactions by Customer to each Cara Partner, Total Transactions, and Scale (=Frequency of Transactions/Total Transactions)

t1 <- SP_Points %>%
  #E-inner join with starting universe
  inner_join(universe3, by="Unique_member_identifier") %>%
  #deduplicated LocationCode, as it seemed like there were multiple locations associated with one location code
  #inner join Cara locations and earn transactions
  inner_join(SP_DimProxyLocation[!duplicated(SP_DimProxyLocation$LocationCode),], by = c("ex_sourceid" = "LocationCode")) %>%
  #select only the following columns: UMI, Points, ex_sourceid (ie. Location Code), BrandeCode and date of transaction
  select(Unique_member_identifier, points, ex_sourceid, BrandCode,pointdt,pointtypeid)%>%
  #inner join the Quality of Activity 
  # inner_join(SP_QualityActivity, by="Unique_member_identifier")%>%
  #filter for all point earning transaction (when points > 0)
  filter(points>0)%>%
  #filter for all CARA activity only
  filter(pointtypeid==1282)%>%
  #filter for active Black Card members in the last 12 months
  #  filter(hasActivity==TRUE)%>%
  #remove all duplicated values in the same day at the same time
  distinct(Unique_member_identifier, points, ex_sourceid, BrandCode,pointdt)%>%
  arrange(Unique_member_identifier)%>%
  na.omit()

detach("package:plyr", unload=TRUE) 

#Create a new column with the total number of transactions by each customer
t2 <- group_by(t1,Unique_member_identifier)%>%
  arrange(Unique_member_identifier)%>%
  summarise(tot=n(),count=n_distinct(BrandCode))%>%
  filter(tot>5)%>%
  filter(count>3)%>%
  na.omit()

#distribution of all transactions
#Create a new column with the total number of transactions by each customer for each cara partner
t3 <- group_by(t1,Unique_member_identifier,BrandCode)%>%
  arrange(Unique_member_identifier)%>%
  na.omit()%>%
  summarise(each=n())

#I believe we had to make the user ID's a descending value from 1 onwards... this is what the functions are for
group_number = (function(){i = 0; function() i <<- i+1 })()
group_number2 = (function(){i = 0; function() i <<- i+1 })()

#creates the ratings file
ratings<-inner_join(t3,t2,by="Unique_member_identifier")%>%
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

#mutate(user = match(Unique_member_identifier, unique(Unique_member_identifier)),item = match(BrandCode, unique(BrandCode)))

#Start building recommender engine

install.packages("recommenderlab")
install.packages("recosystem")

library(devtools)
install_github(repo = "SlopeOne", username = "tarashnot")
install_github(repo = "SVDApproximation", username = "tarashnot")

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

model <- Recommender(real_ratings, method = "POPULAR", param=list(normalize = "center"))

prediction <- predict(model, real_ratings[1:5], type="ratings")
as(prediction, "matrix")[,1:9]
#max of [1:9]

set.seed(5)
e <- evaluationScheme(real_ratings, method="split", train=0.8, given=-1)
#5 ratings of 20% of users are excluded for testing, try with 70/30

model <- Recommender(getData(e, "train"), "POPULAR")
prediction <- predict(model, getData(e, "known"), type="ratings")

rmse_popular <- calcPredictionAccuracy(prediction, getData(e, "unknown"))[1]
rmse_popular

#CF - User Based

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

#recosystem

data(ratings)

set.seed(1)
in_train <- rep(TRUE, nrow(ratings))
in_train[sample(1:nrow(ratings), size = round(0.2 * length(unique(ratings$user)), 0) * 5)] <- FALSE

ratings_train <- ratings[(in_train)]
ratings_test <- ratings[(!in_train)]

write.table(ratings_train, file = "trainset.txt", sep = " ", row.names = FALSE, col.names = FALSE)
write.table(ratings_test, file = "testset.txt", sep = " ", row.names = FALSE, col.names = FALSE)

r = Reco()

opts <- r$tune("trainset.txt", opts = list(dim = c(1:20), lrate = c(0.05),
                                           nthread = 4, cost = c(0), niter = 200, nfold = 10, verbose = FALSE))

r$train("trainset.txt", opts = c(opts$min, nthread = 4, niter = 500, verbose = FALSE))

outfile = tempfile()

r$predict("testset.txt", outfile)

scores_real <- read.table("testset.txt", header = FALSE, sep = " ")$V3
scores_pred <- scan(outfile)

rmse_mf <- sqrt(mean((scores_real-scores_pred) ^ 2))

rmse_mf
