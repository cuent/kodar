library(ggbiplot)

pathWeights <- "/Users/cuent/NetbeansProjects/KODAR/Job/mahout-base/sparse/tfidf.csv"
weights <- read.csv(pathWeights, header = 
                      FALSE, sep = ",")
weights.pca <- prcomp(weights, center = TRUE, scale. = TRUE)
print(weights.pca)
plot(weights.pca, type = "l")
summary(weights.pca)
g <- ggbiplot(weights.pca, var.scale = 1, obs.scale = 1, 
              var.axes = FALSE)
g <- g + theme(legend.direction = 'horizontal', 
               legend.position = 'top')
print(g)

pathToSave <- "Desktop/R Scripts/vectors/vectors.png"
ggsave(pathToSave)
