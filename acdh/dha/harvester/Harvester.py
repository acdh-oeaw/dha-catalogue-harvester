import argparse
import datetime
import logging
import requests
import rdflib
import sys
import tempfile
import urllib
import xml.etree.ElementTree as ET


def run():
    parser = argparse.ArgumentParser(description='OAI-PMH harvester for the DHA Catalogue service')
    parser.add_argument('oaipmhConnectionUrl', help='OAI-PMH connection URL in a form of "{OAI-PMH endpoint URL}#{metadataPrefix}#{set name (optional)}"')
    parser.add_argument('sparqlUrl', help="Triplestore's SPARQL endpoint URL")
    parser.add_argument('--timeout', type=int, default=1800, help='OAI-PMH request timeout (in seconds)')
    parser.add_argument('--sparqlUser', help='HTTP basic auth user name to be used when communicating with the triplestore')
    parser.add_argument('--sparqlPswd', help='HTTP basic auth password to be used when communicating with the triplestore')
    parser.add_argument('--sparqlBatchSize', type=int, default=150, help='Maximum SPARQL UPDATE query size (in kB)')
    parser.add_argument('--tmpDir', default='.', help='Directory where harvested RDF data are stored before ingesting them into the triplestore')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG if args.verbose else logging.INFO)

    harvester = Harvester(args)
    harvester.harvest()

class Harvester:
    oaipmhNmsp = 'http://www.openarchives.org/OAI/2.0/'
    rdfNmsp = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'

    oaipmhUrl = None
    metadataPrefix = None
    setName = None
    timeout = None
    sparqlUrl = None
    sparqlAuth = None
    sparqlMode = None
    sparqlBatchSize = None
    sparqlGraph = None
    tmpDir = None

    def __init__(self, args):
        tmp = args.oaipmhConnectionUrl.split('#')
        self.oaipmhUrl = tmp[0]
        self.metadataPrefix = tmp[1]
        self.setName = tmp[2] if len(tmp) > 2 else ''

        self.timeout = args.timeout
        self.sparqlUrl = args.sparqlUrl
        self.sparqlBatchSize = args.sparqlBatchSize * 1024

        self.tmpDir = args.tmpDir

        if args.sparqlUser != '' and args.sparqlPswd != '':
            self.sparqlAuth = requests.auth.HTTPBasicAuth(args.sparqlUser, args.sparqlPswd)

        self.sparqlGraph = rdflib.term.URIRef(self.oaipmhUrl).n3()

    def harvest(self):
        triplesQueue = tempfile.TemporaryFile(dir=self.tmpDir)
        
        response = self.makeOaipmhRequest('ListIdentifiers', set=self.setName)
        if response is None:
            logging.error('No records found')
            return
        n = 0
        N = len(response)
        logging.info('  %d records found' % len(response))
        if N == 0:
            return

        t0 = datetime.datetime.now()
        for record in response:
            t = (datetime.datetime.now() - t0).total_seconds()
            n += 1
            logging.info('----------')
            logging.info('Processing record %d/%d (%d%% elapsed %d s ETA %d s)' % (n, N, 100 * n / N, t, N * t / n - t))
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
            rdfxml = ET.tostring(rdfxml, encoding='UTF-8', method='xml').decode('UTF-8')
            del recordxml

            graph = rdflib.Graph()
            try:
                graph.parse(data=rdfxml, format='xml')
                triplesQueue.write(graph.serialize(format='nt').encode('UTF-8'))
            except rdflib.exceptions.ParserError as e:
                logging.error('  Error while parsing metadata as RDF-XML: %s' % str(e))
                logging.error(rdfxml)

        logging.info("Removing data for the %s graph from the triplestore" % self.oaipmhUrl)
        query = """
            WITH %s
            DELETE { ?s ?p ?o }
            WHERE { ?s ?p ?o }
        """ % self.sparqlGraph
        res = self.makeSparqlRequest('update', query)
        if res == False:
            return
        logging.info('  Removed')

        self.insertTriples(triplesQueue)


    def insertTriples(self, infile):
        totalLen = infile.tell()
        infile.seek(0)
        batch = ''
        batchLen = 0
        sentLen = 0
        t0 = datetime.datetime.now()
        for line in infile:
            t = (datetime.datetime.now() - t0).total_seconds()
            lineLen = len(line)
            line = line.decode('UTF-8')
            if batchLen + lineLen > self.sparqlBatchSize:
                sentLen += batchLen
                logging.info("Sending triples batch to the triplestore (%d%% elapsed %d s ETA %d s)" % (100 * sentLen / totalLen, t, totalLen * t / sentLen - t))
                query = "INSERT DATA { GRAPH " + self.sparqlGraph + " { " + batch + " } }"
                res = self.makeSparqlRequest('update', query)
                if res == False:
                    break
                batch = ''
                batchLen = 0
            batch += line
            batchLen += lineLen
        logging.debug("Sending triples batch to the triplestore (%d/%d %d%%)" % (totalLen, totalLen, 100))
        query = "INSERT DATA { GRAPH " + self.sparqlGraph + " { " + batch + " } }"
        res = self.makeSparqlRequest('update', query)

    def makeSparqlRequest(self, operation, query):
        data = {}
        data[operation] = query
        logging.debug('Querying %s?%s={query}' % (self.sparqlUrl, operation))
        response = requests.post(self.sparqlUrl, data=data, auth=self.sparqlAuth)
        if response.status_code != 200:
            logging.error('  Triplestore communication error with code %d and message: %s' % (response.status_code, response.text))
            print("\n\n"+query+"\n\n")
            return False
        return True

    def makeOaipmhRequest(self, verb, **kwargs):
        xml = None
        nnsp = {'': 'http://www.openarchives.org/OAI/2.0/'}

        reqStr = '%s?verb=%s&metadataPrefix=%s' % (self.oaipmhUrl, verb, self.metadataPrefix)
        param = {'verb': verb, 'metadataPrefix': self.metadataPrefix}
        for key, value in kwargs.items():
            if value is not None:
                param[key] = value
                reqStr += '&%s=%s' % (key, urllib.parse.quote(value))

        logging.info('Requesting %s' % reqStr)
        try:
            rdfxml = ''
            with requests.get(self.oaipmhUrl, params=param, timeout=self.timeout, stream=True) as response:
                if response.status_code != 200:
                    logging.error('  Request failed with code %d and message: ' % (response.status_code, response.text))
                    return None
                for chunk in response.iter_content(1000000, True):
                    rdfxml += chunk
            try:
                xml = ET.fromstring(rdfxml)
            except xml.etree.ElementTree.ParseError as e:
                logging.error('  Response is not a valid XML:\n%s' % response.text)
                return None

            error = xml.find('error', {'': Harvester.oaipmhNmsp})
            if error is not None:
                logging.error('   Wrong OAI-PMH request: %s' % error.text)
                return None

            return xml.find(verb, {'': Harvester.oaipmhNmsp})
        except requests.exceptions.ReadTimeout:
            logging.error('  Timeout of %d seconds exceeded' % self.timeout)
            return None
