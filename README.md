# KODAR: Clustering application 

The aim of this application is to discover potential areas of knowledge and similar research areas based on keywords of research papers. 

# Data
 The data is extracted from an [SPARQL endpoint](http://190.15.141.85:8080/sparql/admin/squebi.html):

You could execute this query to get last results: 

```sparql
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX bibo: <http://purl.org/ontology/bibo/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?name ?author  ?publication ?title (group_concat(distinct ?keyword;separator=", ") as ?keywords)
WHERE {
  graph <http://ucuenca.edu.ec/wkhuska> {
  	?author foaf:name ?name.
	?author foaf:publications ?publication.  
	?publication dct:title ?title.
  	?publication bibo:Quote ?keyword.
  }
} GROUP BY ?name ?author  ?publication ?title 
```

For the experiment I try with this [dataset]().

# Data Analysis

# Evaluation

# Contribution
 
# Try yourself

**System Requirements**

* Java JRE 1.7.0_45 or superior
* Maven 3 or superior
* Java Application Server (Tomcat 7.x, Jetty 6.x or GlassFish 4.x)

