data <- read.csv(file="/Users/cuent/NetBeansProjects/kodar/mahout-base/kmeans/clusters.csv",header = TRUE,sep=",",stringsAsFactors = F)
data=data[c(1:6896)]
data[c(1:6896)] <- 
  lapply( data[c(1:6896)], 
          function(x) { as.integer(gsub(',', '', x) )})
pca_existing <- prcomp(data, scale. = TRUE)
plot(pca_existing)
scores_existing_df <- as.data.frame(pca_existing$x)
# Show first two PCs
head(scores_existing_df[1:2])
plot(PC1~PC2, data=scores_existing_df, 
     main= "Keywords",
     cex = .1, lty = "solid",col=data$X1182)
legend(7,4.3,unique(data$X1182),col=1:length(data$X1182),pch=1)