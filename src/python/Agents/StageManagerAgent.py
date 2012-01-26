'''
Poll a Couch instance for files to stage
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
import copy

from xml.sax import parseString
from xml.sax.handler import ContentHandler

from StageManager.Agents import AgentConfig

class PhEDExHandler(ContentHandler):
    def __init__(self, function_dict):
        self.function_dict = function_dict

    def startElement(self, name, attrs):
        if name in self.function_dict.keys():
            self.function_dict[name](attrs)

class StageManagerAgent:
    def __init__(self, options, logger, defaults):
        self.defaults = defaults
        self.logger = logger

        # Format the site name
        self.site = options.site.lower()
        self.node = options.site.replace('_Buffer', '_MSS')
        if not self.node.endswith('_MSS'):
            self.node += '_MSS'

        # Connect to local couch
        self.localcouch = CouchServer(options.localdb)

        # Attempt to load the configuration
        self.configdb = self.localcouch.connectDatabase('%s_configuration' % self.site)
        self.load_config()

        # Now update configuration with command line parameters or defaults
        for key in defaults:
            storeName = defaults[key]['opts']['dest']
            # Overwrite those options passed on the command line
            cmdOption = getattr(options, storeName)
            if cmdOption:
                print "Updating config", storeName, cmdOption
                self.config[storeName] = cmdOption
            # Use defaults for any options which remain unset
            if not self.config.has_key(storeName):
                print "Using default config", storeName, defaults[key]['opts']['default']
                self.config[storeName] = defaults[key]['opts']['default']

        # Check waittime is sensible
        if self.config['waittime'] < 31:
            self.config['waittime'] = 31

        # Save the active configuration
        self.save_config()

        # Finish up remote DB connection, and other config
        self.logger.info('local databases: %s' % self.localcouch)

        #Create our stager
        factory = WMFactory('stager_factory', 'StageManager.Agents.Stagers')
        queuedb = self.localcouch.connectDatabase('%s_stagequeue' % self.site)
        statsdb = self.localcouch.connectDatabase('%s_statistics' % self.site)
        requestdb = self.localcouch.connectDatabase('%s_requests' % self.site)

        # Parse stager options
        sopts = self.config['stageroptions'].split(",")
        def OptFilter(opt):
            return opt.find("=") > 0
        sopts = filter(OptFilter, sopts)
        stagerOptions = {}
        for sopt in sopts:
            tokens = sopt.split("=")
            stagerOptions[tokens[0]] = tokens[1]

        self.stager = factory.loadObject(self.config['stagerplugin'],
                                         args=[queuedb, statsdb, self.configdb, requestdb,
                                               stagerOptions, self.logger, self.site],
                                         listFlag = True)

    def save_config(self):
        """
        Write the given configuration to a configuration database.
        """
        # Load existing config (for rev) or create new document
        dbConfig = {"_id" : "agent"}
        if self.configdb.documentExists("agent"):
            dbConfig = self.configdb.document("agent")

        # Save all persistable parameters
        for key in self.defaults:
            if self.defaults[key]['persist'] == True:
                storeName = self.defaults[key]['opts']['dest']
                if self.config.has_key(storeName):
                    dbConfig[storeName] = self.config[storeName]
        self.configdb.commitOne(dbConfig)

    def load_config(self):
        """
        Reads the configuration from the configuration database.
        """
        if not hasattr(self, "config"):
            self.config = {}

        # Check a configuration document exists
        if not self.configdb.documentExists("agent"):
            return

        # Load existing configuration
        dbConfig = self.configdb.document("agent")
        for key in self.defaults:
            if self.defaults[key]['persist'] == True:
                storeName = self.defaults[key]['opts']['dest']
                # We check the key is present in DB to ensure we don't attempt
                # to load a newly defined configuration item
                if dbConfig.has_key(storeName):
                    self.config[storeName] = dbConfig[storeName]

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
        """
        Expand new requests into files to stage and process the existing stage
        queue.
        TODO: parallelise these two calls using multiprocessing
        """
        # Load the config from DB, in case it has been updated
        self.load_config()

        # Process stuff!
        self.proces_requests()
        self.process_stagequeue()

    def check_requests_done(self):
        """
        Checks requests that are complete, and marks them as such
        Should it delete them like done files?
        """
        db = self.localcouch.connectDatabase('%s_requests' % self.site)
        # Get requests, mark them as acquired
        data = {'rows':[]}
        try:
            data = db.loadView('requests', 'request_state', {'reduce':False, 'include_docs':True, 'key':'acquired'})
        except httplib.HTTPException, he:
            self.handleHTTPExcept(he, 'could not retrieve request_state view')
            sys.exit(1)
        if len(data['rows']) > 0:
            all_requests = sanitise_rows(data["rows"])
            for request in all_requests:
                # Get request status
                request_progress = self.query_request_progress(request['_id'])
                if request.has_key('total_files') and request_progress.has_key(request['_id']):
                    if request_progress[request['_id']]['good'] == request['total_files']:
                        self.logger.info("Request %s done" % request['data'])
                        request['state'] = 'done'
                        request['done_timestamp'] = time.time()
                        db.queue(request)
            db.commit(viewlist=['requests/request_state'])

    def query_request_progress(self, request):
        """
        Queries the progress of a request, or all requests, from the
        statistics DB
        """
        db = self.localcouch.connectDatabase('%s_statistics' % self.site)
        # Get requests, mark them as acquired
        data = {'rows':[]}
        try:
            data = db.loadView('statistics', 'request_progress', {'key':request,'reduce':True, 'group_level':1})
        except httplib.HTTPException, he:
            self.handleHTTPExcept(he, 'could not retrieve request_progress view')
            sys.exit(1)
        if len(data['rows']) > 0:
            all_progress = sanitise_reduced_rows(data["rows"])
            return all_progress
        return {}

    def proces_requests(self):
        """
        Connect to the requests database, find the new requests and pass them on
        to process_files to expand from PhEDEx.
        """
        db = self.localcouch.connectDatabase('%s_requests' % self.site)
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
            now = time.time()
            for request in all_requests:
                if request.has_key('due') and now > request['due']:
                    #This request has expired - mark it
                    request['state'] = 'expired'
                    request['accept_timestamp'] = time.time()
                    request['expired_timestamp'] = time.time()
                    self.logger.info("Request for %s has expired" % request['data'])
                else:
                    # expand the files associated with the request
                    request['total_files'], request['total_size'] = self.process_files(request['data'], request['_id'])
                    # mark the request as acquired
                    request['state'] = 'acquired'
                    request['accept_timestamp'] = time.time()
                db.queue(request)
        db.commit(viewlist=['requests/request_state'])

    def process_files(self, stage_data = [], request_id=''):
        """
        Contact PhEDEx data service to get a list of files for a given request.
        TODO: use Service.PhEDEx
        """
        # TODO: make the phedex URL a configurable!
        phedex = Requests(url='https://cmsweb.cern.ch', idict={'accept_type':'application/json'})

        totalFiles = 0
        totalBytes = 0
        # Need to clean up the input a bit
        for d in stage_data:
            qParams = {'node': self.node}
            # Need to pass in blocks
            if d.find('#') < 0:
                qParams['dataset'] = d
            else:
                qParams['block'] = d

            try:
               pdata = phedex.get('/phedex/datasvc/json/prod/filereplicas', qParams)[0]
            except httplib.HTTPException, he:
               self.handleHTTPExcept(he, 'HTTPException for block: %s node: %s' % (d, self.node))

            # Parse block info
            try:
                p = JsonWrapper.loads(pdata)
                if len(p['phedex']['block']) == 0:
                   self.logger.error('Block %s not resident at site %s' % (d, self.node))
                else:
                   self.logger.debug('Creating stage-in requests for %s' % self.node)
                   db = self.localcouch.connectDatabase('%s_stagequeue' % self.site)
                   for blk in p['phedex']['block']:
                      self.logger.debug('Creating stage-in requests for block %s' % blk['name'])
                      for fle in blk['file']:
                         self.logger.debug('Creating stage-in requests for file %s' % fle['name'])
                         checksum = fle['checksum']
                         f = {'_id': fle['name'],
                              'bytes': int(fle['bytes']),
                              'checksum': {checksum.split(':')[0]: checksum.split(':')[1]},
                              'state': 'new',
                              'retry_count': [],
                              'request_id': request_id
                         }
                         try:
                            db.queue(f, timestamp = True, viewlist=['stagequeue/file_state'])
                            totalFiles += 1
                            totalBytes += f['bytes']
                         except httplib.HTTPException, he:
                            self.handleHTTPExcept(he, 'Could not commit data')
            except:
                self.logger.debug('error loading json')

        try:
            db.commit(viewlist=['stagequeue/file_state'])
        except httplib.HTTPException, he:
            self.handleHTTPExcept(he, 'Could not commit data')
            totalFiles = 0
            totalBytes = 0

        return totalFiles, totalBytes

    def process_stagequeue(self):
        """
        Send work to the stager
        """
        db = self.localcouch.connectDatabase('%s_stagequeue' % self.site)
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
                if self.config['maxstage'] <= 0:
                    self.stager(data)
                else:
                    c = 0
                    lower = 0
                    upper = self.config['maxstage']
                    while c < len(data):
                        #TODO: subprocess #31
                        self.stager(data[lower:upper])
                        c = upper
                        lower = upper
                        upper += self.config['maxstage']
        else:
            self.logger.info('Nothing to do, sleeping for %s seconds' % (self.config['waittime'] - 30))
            time.sleep(self.config['waittime'] - 30)
        self.check_requests_done()
        time.sleep(30)
        logger.debug('compacting database')
        try:
            db.compact(['stagequeue'])
        except httplib.HTTPException, he:
            self.handleHTTPExcept(he, 'could not compact local database')

def do_options(defaults):
    """
    Read the users arguments and set sensible defaults
    """
    # Configure the options from the passed in list
    op = OptionParser()
    for key in defaults:
        # Deep copy so we don't overwrite the default
        cur = copy.deepcopy(defaults[key])
        if cur['persist'] == True:
            cur['opts']['default'] = None
        op.add_option(key, cur['long'], **cur['opts'])

    # Run the option parser
    options, args = op.parse_args()

    # Configure the logger
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger('StageManager')
    if options.verbose:
        logger.setLevel(logging.INFO)
    if options.debug:
        logger.setLevel(logging.DEBUG)

    # Sanity checks
    logger.info('options: %s, args: %s' % (options, args))
    if not cmsname(options.site):
        logger.warning('%s is not a valid CMS name!' % options.site)
        sys.exit(101)
    elif not options.site.startswith('T1'):
        logger.warning('%s is not a T1 site' % options.site)
        sys.exit(102)

    return options, args, logger

def sanitise_rows(rows):
    """
    Quick n dirty method to make a list of docs
    """
    sanitised_data = []
    for i in rows:
        if 'doc' in i.keys():
            sanitised_data.append(i['doc'])
    return sanitised_data

def sanitise_reduced_rows(rows):
    sanitised_data = {}
    for i in rows:
        if 'key' in i.keys():
            sanitised_data[i['key']] = i['value']
    return sanitised_data

if __name__ == '__main__':
    opts, args, logger = do_options(AgentConfig.defaultOptions)

    agent = StageManagerAgent(opts, logger, AgentConfig.defaultOptions)

    #TODO: This should be a deamon #27
    while True:
        agent()
