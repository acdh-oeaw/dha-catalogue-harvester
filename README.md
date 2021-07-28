# DHA Catalogue Harvester

An OAI-PMH harverster for the DHA Catalogue.

## Installation

* Obtain the [poetry](https://python-poetry.org/docs/#installation)
* Clone this repo and enter it
* Run `poetry install` to install all dependencies
* Run `poetry run dha-harvester '{OAI-PMH URL}#{OAI-PMH metadataPrefix}#{optional OAI-PMH set name}'`, e.g.  
  ```
  poetry run dha-harvester 'https://arche.acdh.oeaw.ac.at/oaipmh/#dha#dha_catalogue_oaipmh_set' \
    'https://triplestore.acdh-dev.oeaw.ac.at/dha-catalogue/sparql' \
     --sparqlUser dha-catalogue --sparqlPswd password
  ````

## Documentation

