data <- read.csv(file="/Users/cuent/NetBeansProjects/kodar/mahout-base/sparse/tfidf.csv",header = TRUE,sep=",",stringsAsFactors = F)
data=data[c(1:2801)]
data[c(1:2801)] <- 
  lapply( data[c(1:2801)], 
          function(x) { as.integer(gsub(',', '', x) )})
pca_existing <- prcomp(data, scale. = TRUE)
plot(pca_existing)
scores_existing_df <- as.data.frame(pca_existing$x)
# Show first two PCs
head(scores_existing_df[1:2])
plot(PC1~PC2, data=scores_existing_df, 
     main= "Keywords",
     cex = .1, lty = "solid")