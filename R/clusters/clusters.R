library(ggbiplot)

pathClusters <- "/Users/cuent/NetbeansProjects/KODAR/Job/target/kodar_home/kmeans/clusters.csv"
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
              labels = clusters.labels)#, ellipse = TRUE,
              #circle = TRUE)
g <- g + scale_color_continuous(name = '')
g <- g + theme(legend.direction = 'horizontal', 
               legend.position = 'top')
print(g)
pathToSave <- "NetbeansProjects/KODAR/R/clusters/clusters.png"
ggsave(pathToSave)
