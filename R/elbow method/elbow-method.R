library(ggplot2)

# The elbow method

pathwss <- "/Users/cuent/NetBeansProjects/KODAR/R/elbow method/wss levels.csv"
wss <- read.csv(pathwss,header = TRUE, sep = ",")
plot <- ggplot(data = wss, aes(x = k, y = interClusterDensity))
plot <- plot + geom_point() + geom_line(color="red")
plot <- plot + labs(x = 'k', y = 'Inter-cluster density')
plot <- plot + scale_color_manual(values=c("#CC6666", "#9999CC"))
plot
ggsave("NetbeansProjects/KODAR/R/elbow method/wss_levels.png")
