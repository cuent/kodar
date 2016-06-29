# Paper of SparseM package http://www.econ.uiuc.edu/~roger/research/sparse/SparseM.pdf

library(reshape2)
library(ggplot2)

library(SparseM)

pathCSV <- "/Users/cuent/NetBeansProjects/KODAR/Job/mahout-base/sparse/tfidf.csv"
data <- read.csv(pathCSV, header = FALSE, sep = ",")
numcol <- ncol(data)
dMatrix <- matrix(unlist(data), ncol = numcol, byrow = TRUE)
dMatrix.csr <- as.matrix.csr(dMatrix)
image(dMatrix.csr, col=c("white","blue"), xlab="x", ylab="y")


ggplot(melt(dMatrix), aes(Var1,Var2, fill=value)) + geom_raster() + scale_fill_gradient2(low='red', high='black', mid='white') + theme_bw() + xlab("x1") + ylab("x2")





set.seed(7)
m <- matrix(0, 100, 100)
n <- 1000
m[sample(length(m), size = n)] <- rep(-1:1, length=n)
m
ggplot(melt(m), aes(Var1,Var2, fill=value)) + geom_raster() + scale_fill_gradient2(low='red', high='black', mid='white') + theme_bw() + xlab("x1") + ylab("x2")




wtmat<-matrix(rnorm(4602*1817),nrow=4602)
library(plotrix)
x11(width=10,height=13)
color2D.matplot(dMatrix,c(1,0),c(1,0),c(1,0),border=FALSE)



# wtmat<-matrix(rnorm(4602*1817),nrow=4602)
# use a smaller matrix to illustrate the principle
wtmat<-matrix(rnorm(46*18),nrow=46)
# make it "sparse" by taking out all small values
# in your case this may be changing all zero values to NS
wtmat[abs(wtmat)<1]<-NA
library(plotrix)
x11(width=5,height=13)
# display all values in the matrix 
# colored as red->white (negative values), white (NA)
# and white->black (positive values)
color2D.matplot(wtmat,c(1,1,0),c(0,1,0),c(0,1,0),border=FALSE)
# now do a plot just showing values that are not NA
color2D.matplot(abs(wtmat),extremes=c(4,4),border=FALSE)

