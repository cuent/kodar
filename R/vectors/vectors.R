library(ggbiplot)

pathWeights <- "/Users/cuent/NetbeansProjects/KODAR/Job/target/kodar_home/sparse/tfidf.csv"
csv <- read.csv(pathWeights, header = 
                      TRUE, sep = ",")
weights <- csv[-1]
weights.label <- csv[1]
weights.pca <- prcomp(weights, center = TRUE, scale. = TRUE)
print(weights.pca)
plot(weights.pca, type = "l")
summary(weights.pca)
g <- ggbiplot(weights.pca, var.scale = 1, obs.scale = 1, 
              var.axes = FALSE, labels = weights.label)
g <- g + theme(legend.direction = 'horizontal', 
               legend.position = 'top')
print(g)

pathToSave <- "NetbeansProjects/KODAR/R/vectors/vectors.png"
ggsave(pathToSave)
