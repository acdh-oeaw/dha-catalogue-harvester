import argparse
import logging
import requests
import rdflib
import sys
import urllib
import xml.etree.ElementTree as ET


def run():
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    parser = argparse.ArgumentParser(description='OAI-PMH harvester for the DHA Catalogue service')
    parser.add_argument('oaipmhConnectionUrl', help='OAI-PMH connection URL in a form of "{OAI-PMH endpoint URL}#{metadataPrefix}#{set name (optional)}"')
    parser.add_argument('--timeout', type=int, default=300, help='OAI-PMH request timeout (in seconds)')
    args = parser.parse_args()

    harvester = Harvester(args)
    harvester.harvest()

class Harvester:
    oaipmhNmsp = 'http://www.openarchives.org/OAI/2.0/'
    rdfNmsp = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'

    baseUrl = None
    metadataPrefix = None
    setName = None
    timeout = None

    def __init__(self, args):
        tmp = args.oaipmhConnectionUrl.split('#')
        self.baseUrl = tmp[0]
        self.metadataPrefix = tmp[1]
        self.setName = tmp[2] if len(tmp) > 2 else ''

        self.timeout = args.timeout

    def harvest(self):
        ids = self.oaipmhListIdentifiers()

    def oaipmhListIdentifiers(self):
        response = self.makeOaipmhRequest('ListIdentifiers', set=self.setName)
        if response is None:
            logging.error('No records found')
            return
        n = 1
        N = len(response)
        logging.info('  %d records found' % len(response))
        for record in response:
            logging.info('----------')
            logging.info('Processing record %d/%d (%d%%)' % (n, N, 100 * n / N))
            idEl = record.find('identifier', {'': Harvester.oaipmhNmsp})
            if idEl is None:
                logging.error('  Wrong OAI-PMH record - misses oaipmh:identifier element')
                continue
            
            recordxml = self.makeOaipmhRequest('GetRecord', identifier=idEl.text)
            if recordxml is None:
                logging.error('  No data found')
                continue
            rdfxml = recordxml.find('./record/metadata/rdf:RDF', {'': Harvester.oaipmhNmsp, 'rdf': Harvester.rdfNmsp})
            if rdfxml is None:
                logging.error('  Wrong OAI-PMH - misses oaipmh:record/oaipmh:metadata/rdf:RDF element(s)')
            rdfxml = ET.tostring(rdfxml, encoding='utf8', method='xml')
            del recordxml
            
            graph = rdflib.Graph()
            try:
                graph.parse(rdfxml, format='xml')
            except rdflib.exceptions.ParserError as e:
                logging.error('  Error while parsing metadata as RDF-XML: %s' % str(e))
                logging.error(rdfxml.decode('utf-8'))

            n += 1

    def makeOaipmhRequest(self, verb, **kwargs):
        xml = None
        nnsp = {'': 'http://www.openarchives.org/OAI/2.0/'}

        reqStr = '%s?verb=%s&metadataPrefix=%s' % (self.baseUrl, verb, self.metadataPrefix)
        param = {'verb': verb, 'metadataPrefix': self.metadataPrefix}
        for key, value in kwargs.items():
            if value is not None:
                param[key] = value
                reqStr += '&%s=%s' % (key, urllib.parse.quote(value))

        logging.info('Requesting %s' % reqStr)
        response = requests.get(self.baseUrl, params=param, timeout=self.timeout)
        if response.status_code != 200:
            logging.error('  Request failed with code %d and message: ' % (response.status_code, response.text))

        try:
            xml = ET.fromstring(response.text)
            del response
        except xml.etree.ElementTree.ParseError as e:
            logging.error('  Response is not a valid XML:\n%s' % response.text)

        error = xml.find('error', {'': Harvester.oaipmhNmsp})
        if error is not None:
            logging.error('   Wrong OAI-PMH request: %s' % error.text)
            xml = None

        return xml.find(verb, {'': Harvester.oaipmhNmsp})

