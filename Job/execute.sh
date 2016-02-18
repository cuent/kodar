cd /Users/cuent/NetBeansProjects/KODAR/Job

$M2_HOME/bin/mvn "-Dexec.args=-classpath %classpath edu.ucuenca.kodar.researchareas.Execute mahout-base/original/authors.csv mahout-base/original/keywords.csv" -Dexec.executable=/Library/Java/JavaVirtualMachines/jdk1.8.0_45.jdk/Contents/Home/bin/java org.codehaus.mojo:exec-maven-plugin:1.2.1:exec