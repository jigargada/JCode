library(data.table)
library(caTools)
library(Amelia)

train <- data.table(read.table("C:/Users/jigar/Desktop/KAGGLE/Titanic dataset/train.csv", 
           header = TRUE, sep = ",", na.strings = "NA"))

train[, sapply(train, function(x) sum(is.na(x)))]
train[, sapply(train, function(x) length(is.na(x)))]
missmap(train, main = "Missing values vs Observed")

# set.seed(88)
# split <- sample.split(train$Recommended, SplitRatio = 0.75)

data <- data.table(train[,c(2,3,5,6,7,8,10,12)])

meann <- train[,round(mean(Age, na.rm=T), digits = 4)]

data <- data[is.na(Age), `:=`(Age = meann)]
# data <- data[lapply(Age, function(x) ifelse(is.na(x), mean(x, na.rm = TRUE), x))]

data[,is.factor(Sex)]
data[,is.factor(Embarked)]
data[,contrasts(Sex)]
data[,contrasts(Embarked)]

data[!is.na(Embarked),]

model <- data[,glm(Survived ~.,family=binomial(link='logit'),data=data)]
summary(model)
