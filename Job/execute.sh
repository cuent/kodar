cd /Users/cuent/NetBeansProjects/KODAR/Job

$M2_HOME/bin/mvn "-Dexec.args=-classpath %classpath edu.ucuenca.kodar.clusters.Execute mahout-base/original/original.csv" -Dexec.executable=$JAVA_HOME/bin/java org.codehaus.mojo:exec-maven-plugin:1.2.1:exec