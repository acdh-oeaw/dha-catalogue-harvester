import argparse
import datetime
import logging
import requests
import rdflib
import sys
import tempfile
import time
import urllib
import xml.etree.ElementTree as ET


def run():
    parser = argparse.ArgumentParser(description='Finds vocabulary concepts (identifying them by a namespace) in a triplestore and enriches the triplestore with full concept definitions fetched from their URIs (assuming requesting concept\'s URI with HTTP Accept text/turtle header will provide concept\'s data in the turtle format)')
    parser.add_argument('sparqlUrl', help="Triplestore's SPARQL endpoint URL")
    parser.add_argument('conceptsNamespace', help="URI namespace of RDF nodes to be processed")
    parser.add_argument('--sparqlUser', help='HTTP basic auth user name to be used when communicating with the triplestore')
    parser.add_argument('--sparqlPswd', help='HTTP basic auth password to be used when communicating with the triplestore')
    parser.add_argument('--sparqlGraph', help='Process only a given triplestore graph')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG if args.verbose else logging.INFO)

    harvester = VocabularyHarvester(args)
    harvester.harvest()

class VocabularyHarvester:
    sparqlUrl = None
    sparqlAuth = None
    sparqlGraph = None
    conceptsNmsp = None

    def __init__(self, args):
        self.sparqlUrl = args.sparqlUrl
        if args.sparqlGraph:
            self.sparqlGraph = rdflib.term.URIRef(args.sparqlGraph).n3()
        self.conceptsNmsp = rdflib.term.Literal(args.conceptsNamespace).n3()

        if args.sparqlUser != '' and args.sparqlPswd != '':
            self.sparqlAuth = requests.auth.HTTPBasicAuth(args.sparqlUser, args.sparqlPswd)

    def harvest(self):
        fromGraph = "from named %s" % self.sparqlGraph if self.sparqlGraph else ''
        query = """
            select distinct ?g ?o %s
            where {
                graph ?g { 
                    ?s ?p ?o 
                    filter strstarts(str(?o), %s)
                }
            }
        """ % (fromGraph, self.conceptsNmsp)
        response = requests.post(self.sparqlUrl, data={"query": query}, headers={"Accept": "application/json"}, auth=self.sparqlAuth)
        if response.status_code != 200:
            logging.error("Failed to find concepts in the triplestore with status code %d and response body: %s" % (response.status_code, response.text))
        data = response.json()
        for i in data['results']['bindings']:
            logging.info("Fetching concept %s" % i['o']['value'])
            try:
                conceptGraph = self.fetchConcept(i['o']['value'])
                #print(conceptGraph.serialize(format='turtle'))
                self.updateTriplestore(conceptGraph, i['g']['value'])
            except Exception as e:
                logging.warning("Failed to fetch data for concept %s:\n    %s" % (i['o']['value'], str(e)))

    def fetchConcept(self, url):
        response = requests.get(url, headers={"Accept": "text/turtle"})
        graph = rdflib.Graph()
        graph.parse(data=response.text, format='turtle')
        return graph

    def updateTriplestore(self, conceptGraph, graph):
        graph = rdflib.term.URIRef(graph).n3()
        query = "INSERT DATA { GRAPH " + graph + " { " + conceptGraph.serialize(format='nt') + " } }"
        response = requests.post(self.sparqlUrl, data={'update': query}, auth=self.sparqlAuth)
        if response.status_code != 200:
            raise Exception("Sending data to the triplestore failed with code %d and response body: %s" % (response.status_code, response.text))

