'''
Poll a Couch instance for files to stage

@author: metson
'''
from optparse import OptionParser
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import cmsname
from WMCore.WMFactory import WMFactory
from WMCore.Services.Requests import Requests
from WMCore.Wrappers import JsonWrapper
import httplib
import urllib
import sys
import time
import datetime
import logging

from xml.sax import parseString
from xml.sax.handler import ContentHandler 

class PhEDExHandler(ContentHandler):
    def __init__(self, function_dict):
        self.function_dict = function_dict
        
    def startElement(self, name, attrs):
        if name in self.function_dict.keys():
            self.function_dict[name](attrs)

class StageManagerAgent:
    def __init__(self, site, localdb, remotedb, logger):
        self.site = site.lower()
        self.node = site.replace('_Buffer', '_MSS')
        if not self.node.endswith('_MSS'):
            self.node += '_MSS'

        self.logger = logger
            
        self.localcouch = CouchServer(localdb)
        self.remotecouch = CouchServer(remotedb)
        
        self.logger.info('local databases: %s' % self.localcouch)
        self.logger.info('remote databases: %s' % self.remotecouch)
        
        self.setup_databases()
        self.initiate_replication()
        self.save_config()
        
        #Create our stager
        factory = WMFactory('stager_factory', 'Agents.Stagers')
        queuedb = self.localcouch.connectDatabase('%s/stagequeue' % self.site)
        statsdb = self.localcouch.connectDatabase('%s/statistics' % self.site)
        self.stager = factory.loadObject(opts.stager, 
                                         args=[queuedb, statsdb, self.logger], 
                                         listFlag = True)
        
    def save_config(self):
        """
        Write the given configuration to a configuration database.
        """
        pass
    
    def setup_databases(self):
        """
        Make sure the databases exist where we expect
        """
        for db in ['/stagequeue', '/statistics', '/requests']:
            db = self.site + db
            try: 
                self.localcouch.connectDatabase(db)
            except httplib.HTTPException, he:
                self.handleHTTPExcept(he, 'Could not contact %s locally' % db)
            try:
                self.remotecouch.connectDatabase(db)
            except httplib.HTTPException, he:
                self.handleHTTPExcept(he, 'Could not contact %s remotely' % db)
        
    def initiate_replication(self):
        """
        Configure and trigger the continuouse replication of databases between
        central and local databases. This will be done by CouchDB itself 'soon',
        at which point this should be removed and replaced with appropriate 
        configuration instructions.
        """
        
        #Set up tasty bi-directional replication for requests...
        
        dbname = '%s/requests' % (self.site)
        dbname = urllib.quote_plus(dbname)
        print dbname, self.localcouch.url, self.remotecouch.url
        try:
            self.localcouch.replicate('%s/%s' % (self.remotecouch.url, dbname), 
                         '%s/%s' % (self.localcouch.url, dbname), 
                         True, True)
        except httplib.HTTPException, he:
            self.handleHTTPExcept(he, 'Could not trigger replication for %s' % dbname)
        try:
            self.localcouch.replicate('%s/%s' % (self.localcouch.url, dbname), 
                        '%s/%s' % (self.remotecouch.url, dbname), 
                         True, True)
        except httplib.HTTPException, he:
            self.handleHTTPExcept(he, 'Could not trigger replication for %s' % dbname)
        
        # ... and one direction replication for statistics
        dbname = '%s/statistics' % (self.site)
        dbname = urllib.quote_plus(dbname)
        print dbname   
        try:
            self.localcouch.replicate('%s/%s' % (self.localcouch.url, dbname), 
                        '%s/%s' % (self.remotecouch.url, dbname), 
                         True, True) 
        except httplib.HTTPException, he:
            self.handleHTTPExcept(he, 'Could not trigger replication for %s' % dbname)
            
    def handleHTTPExcept(self, he, message):
        """
        Some crude exception handling, just log the problem and move on...
        """
        self.logger.error(message)
        self.logger.info(he.status)
        self.logger.info(he.result)
        self.logger.info(he.reason)
        self.logger.info(he.message)
    
    def __call__(self):
        # Probably makes sense for these to be subprocesses.
        self.proces_requests()
        self.process_stagequeue()

    def proces_requests(self):
        db = self.localcouch.connectDatabase('%s/requests' % self.site)
        # Get requests, mark them as acquired
        data = {'rows':[]}
        try:
            data = db.loadView('requests', 'request_state', {'reduce':False, 'include_docs':True, 'key':'new'})
        except httplib.HTTPException, he:
            self.handleHTTPExcept(he, 'could not retrieve request_state view')
            sys.exit(1) 
        if len(data['rows']) > 0:
            all_requests = sanitise_rows(data["rows"])
            # [{'timestamp': '2010-04-26 17:17:54.166314', '_rev': '1-cd935f55f4bc1ff4b54a2551bf37dc0e', '_id': 'f52a38ae152965593dbdf03a9800828a', 'data': ['/MinBias/Summer09-MC_31X_V3_7TeV-v1/GEN-SIM-RECO', '/QCD_pt_0_15/JobRobot_IDEAL_V9_JobRobot/GEN-SIM-RAW-RECO'], 'state': 'new'}]
            now = time.mktime(datetime.datetime.now().timetuple())
            for request in all_requests:
                if request.has_key('due') and now > request['due']:
                    #This request has expired
                    continue
                # expand the files associated with the request
                self.process_files(request['data'], request['_id'])
                # mark the request as acquired
                request['state'] = 'acquired'
                request['accept_timestamp'] = str(datetime.datetime.now())
                db.queue(request)
        db.commit(viewlist=['requests/request_state'])

      
    def process_files(self, stage_data = [], request_id=''):
        # Need to clean up the input a bit
        for d in stage_data:
            # Need to pass in blocks
            if d.find('#') < 0:
                stage_data[stage_data.index(d)] = d + '*'
        # TODO: make the phedex URL a configurable!
        phedex = Requests(url='https://cmsweb.cern.ch', dict={'accept_type':'text/xml'})
        self.logger.debug('Creating stage-in requests for %s' % self.node)
        
        db = self.localcouch.connectDatabase('%s/stagequeue' % self.site)
        
        try:
            data = phedex.get('/phedex/datasvc/xml/prod/filereplicas', {
                                                    'block':stage_data, 
                                                    'node': self.node})[0]
        except httplib.HTTPException, he:
            self.handleHTTPExcept(he, 'HTTPException for block: %s node: %s' % (data, self.node))
                
        #<file checksum='cksum:2470571517' bytes='1501610356' 
        #name='/store/mc/Summer09/MinBias900GeV/AODSIM/MC_31X_V3_AODSIM-v1/0021/F0C49EA2-FA88-DE11-B886-003048341A94.root' 
        #id='29451522' origin_node='T2_US_Wisconsin' time_create='1250711698.34438'><replica group='DataOps' node_id='19' se='srm-cms.gridpp.rl.ac.uk' custodial='y' subscribed='y' node='T1_UK_RAL_MSS' time_create=''/></file>
        
        def file_sax_test(attrs):
            checksum = attrs.get('checksum')
            f ={'_id': attrs.get('name'),
                'bytes': int(attrs.get('bytes')),
                'checksum': {checksum.split(':')[0]: checksum.split(':')[1]},
                'state': 'new',
                'retry_count': [],
                'request_id': request_id
                }
            try:
                db.queue(f, timestamp = True, viewlist=['stagequeue/file_state'])
            except httplib.HTTPException, he:
                self.handleHTTPExcept(he, 'Could not commit data')
        
        saxHandler = PhEDExHandler({'file': file_sax_test})
        parseString(data, saxHandler)
        try:
            db.commit(viewlist=['stagequeue/file_state'])
        except httplib.HTTPException, he:
            self.handleHTTPExcept(he, 'Could not commit data')    

    def process_stagequeue(self):
        db = self.localcouch.connectDatabase('%s/stagequeue' % self.site)
        #TODO: hit view for size of backlog, stop replication if over some limit #28
        data = {'rows':[]}
        try:
            data = db.loadView('stagequeue', 'file_state', {'reduce':False, 'include_docs':True})
        except httplib.HTTPException, he:
            self.handleHTTPExcept(he, 'could not retrieve file_state view')
            sys.exit(1)
        if len(data['rows']) > 0:
            data = sanitise_rows(data["rows"])
            if len(data) > 0:
                if opts.maxstage < 0:
                    self.stager(data)
                else:
                    c = 0
                    lower = 0
                    upper = opts.maxstage
                    while c < len(data):
                        #TODO: subprocess #31
                        stager(data[lower:upper])
                        c = upper
                        lower = upper - 1
                        upper += opts.maxstage 
        else:
            self.logger.info('Nothing to do, sleeping for five minutes')
            time.sleep(270)            
        time.sleep(30)
        logger.debug('compacting database')
        try:
            db.compact(['stagequeue'])
        except httplib.HTTPException, he:
            self.handleHTTPExcept(he, 'could not compact local database')
    
def do_options():
    op = OptionParser()
    op.add_option("-u", "--local-url",
              dest="local", 
              help="Local CouchDB url. Default address http://127.0.0.1:5984", 
              default="http://127.0.0.1:5984")
    op.add_option("-r", "--remote-url",
              dest="remote", 
              help="Remote CouchDB url. Default address cmsweb.cern.ch/stager", 
              default="cmsweb.cern.ch/stager")
    op.add_option("-s", "--site",
              dest="site", 
              help="Name of the site the agent is running for", 
              default="T1_UK_RAL")
    op.add_option("-v", "--verbose",
              dest="verbose", 
              action="store_true",
              default=False, 
              help="Be more verbose")
    op.add_option("-d", "--debug",
              dest="debug", 
              action="store_true",
              default=False, 
              help="Be extremely verbose - print debugging statements")
    op.add_option("-m", "--maxstage",
              dest="maxstage", 
              default=-1, 
              type='int',
              metavar="NUM",
              help="Send the stager NUM requests at a time (default is -1: stage all files)")
    op.add_option("-p", "--persist",
              dest="persist", 
              action="store_true",
              default=False, 
              help="Persist settings to the configuration DB")
    op.add_option("-l", "--load",
              dest="load", 
              action="store_true",
              default=False, 
              help="Load settings from the configuration DB")
    op.add_option("--stager",
              dest="stager",
              default="FakeStager",
              help="name of stager")
    
    #TODO: persist options to local couch, pick them up #29
    options, args = op.parse_args()
    
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger('StageManager')
    if options.verbose:
        logger.setLevel(logging.INFO)
    if options.debug:
        logger.setLevel(logging.DEBUG)

    
    if options.load:
        # Load options from the DB
        pass

    logger.info('options: %s, args: %s' % (options, args))

    if not cmsname(options.site):
        logger.warning('%s is not a valid CMS name!' % options.site)
        sys.exit(101)
    elif not options.site.startswith('T1'):
        logger.warning('%s is not a T1 site' % options.site)
        sys.exit(102)
    
    if options.persist:
        # Write the options to the config DB
        pass
    
    return options, args, logger

def sanitise_rows(rows):
    sanitised_data = []
    for i in rows:
        if 'doc' in i.keys():
            sanitised_data.append(i['doc'])
    return sanitised_data

if __name__ == '__main__':
    opts, args, logger = do_options()
    
    agent = StageManagerAgent(opts.site, opts.local, opts.remote, logger)
    
    #TODO: This should be a deamon #27
    while True:
        agent()