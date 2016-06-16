library(ggbiplot)

pathClusters <- "/Users/cuent/NetbeansProjects/KODAR/Job/mahout-base/kmeans/clusters.csv"
clusters <- read.csv(pathClusters, 
                     header = FALSE, sep = ",")
clusters.labels <- clusters[,1]
clusters.pca <- prcomp(clusters[,2:ncol(clusters)], 
                       center = TRUE, scale. = TRUE)

print(clusters.pca)
plot(clusters.pca, type = "l")
summary(clusters.pca)
g <- ggbiplot(clusters.pca, var.scale = 1, obs.scale = 1, 
              var.axes = FALSE, groups = clusters.labels,
              labels = clusters.labels)
g <- g + scale_color_continuous(name = '')
g <- g + theme(legend.direction = 'horizontal', 
               legend.position = 'top')
print(g)
pathToSave <- "Desktop/R Scripts/clusters/clusters.png"
ggsave(pathToSave)
