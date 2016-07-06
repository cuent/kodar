# Preprocess
library(tm)
text <- readLines("/Users/cuent/NetbeansProjects/KODAR/Job/target/kodar_home/raw/keywords.csv")

# Build Corpus
text <- iconv(text,to="utf-8-mac")
corpus <- Corpus(VectorSource(text))

# Clean text
corpus <- tm_map(corpus,tolower,lazy = TRUE)
corpus <- tm_map(corpus,removePunctuation,lazy=TRUE)
corpus <- tm_map(corpus,removeNumbers,lazy=TRUE)
#corpus <- tm_map(corpus,removeWords,stopwords("english"),lazy=TRUE)
#corpus <- tm_map(corpus,gsub,pattern="words",replacement="word")
clean <- tm_map(corpus, stripWhitespace, lazy = TRUE)

# Term Document Matrix
clean <- tm_map(clean, PlainTextDocument, lazy=TRUE)
tdm <- TermDocumentMatrix(clean, control = list(minWordLength=c(1,Inf)))
tdm <- as.matrix(tdm)

# Bar Plot
termFrequency <- rowSums(tdm)
termFrequency <- subset(termFrequency, termFrequency>3)
barplot(termFrequency,las=2, col=rainbow(100))

# Wordcloud
library(wordcloud)
wordFreq <- sort(rowSums(tdm), decreasing = TRUE)
set.seed(123)
wordcloud(words = names(wordFreq), freq=wordFreq, random.order = F
          #,max.words = 5
          #, min.freq = 3
          ,col= brewer.pal(6,'Dark2')
          #,scale = c(7,41) 
          ,rot.per = 0.5
          )
