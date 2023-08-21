# Gallery of SPARQL queries

TODO Create a repository of example queries like in [Wikidata](https://query.wikidata.org/) (see "Examples" button).

[SPARQL By Example](https://www.w3.org/2009/Talks/0615-qbe/)

[SPARQL Tutorial (Apache Jena)](https://jena.apache.org/tutorials/sparql.html)

## List email attachments

```sparql
PREFIX sc: <http://purl.org/science/owl/sciencecommons/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <https://schema.org/>

SELECT *
WHERE {
  ?message rdf:type schema:EmailMessage .
  ?message schema:dateSent ?date .
  ?message schema:sender ?sender .
  ?message schema:subject ?subject .
  ?message schema:attachment ?attachment .
  ?message schema:recipient ?to .
  ?attachment rdf:type schema:DataDownload .
  ?attachment schema:name ?name .
  ?attachment schema:encodingFormat ?format . 
  FILTER ( ?format NOT IN ("image/png", "image/jpeg") )
}
ORDER BY DESC(?date)
```

## Number of web visits by month and browser

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX b: <https://mydata-schema.org/browser/>
PREFIX m: <mydata://>

SELECT ?yearMonth ?browser (COUNT(DISTINCT ?obj) AS ?visits) (COUNT(DISTINCT ?url) AS ?urls)
WHERE {
    ?obj rdf:type m:WebVisit .
    ?obj m:browser ?browser .
    ?obj m:url ?url .
    ?obj m:time ?time .
    BIND(CONCAT(STR(YEAR(?time)), "-", STR(MONTH(?time))) AS ?yearMonth)
}
GROUP BY ?yearMonth ?browser
ORDER BY ?yearMonth ?browser
LIMIT 100
```

## Number of google takeout events by month

```sparql
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX g: <https://mydata-schema.org/google/>
PREFIX m: <mydata://>

SELECT ?yearMonth (COUNT(DISTINCT ?obj) AS ?visits) (COUNT(DISTINCT ?url) AS ?urls)
WHERE {
    ?obj rdf:type g:Activity .
    ?obj g:url ?url .
    ?obj g:time ?time .
    BIND(CONCAT(STR(YEAR(?time)), "-", STR(MONTH(?time))) AS ?yearMonth)
}
GROUP BY ?yearMonth
ORDER BY DESC(?yearMonth)
```
