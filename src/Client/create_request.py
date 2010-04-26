'''
Created on 20 Apr 2010

Push a list of files (see below) into a Couch server based on a query to PhEDEx. 

file = {
    '_id': 'lfn',
    'size': file size,
    'checksum': {'type':'checksum'}
}

py create_request.py -v -d /MinBias/Summer09-MC_31X_V3_7TeV-v1/GEN-SIM-RECO \
    -s T1_UK_RAL -d /MinBias900GeV/Summer09-MC_31X_V3_AODSIM-v1/AODSIM \
    -u 192.168.179.134:5984

'''

from optparse import OptionParser
import logging
import os
import pwd
import sys
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import cmsname

class StageManagerClient:
    def __init__(self, dburl, logger):
        self.couch = CouchServer(dburl)
        self.logger = logger
        
    def store_request(self, sites = [], stage_data = []):
        self.logger.info('Requesting %s' % stage_data)
        doc = {'data': stage_data, 'state': 'new'}
        
        for site in sites:
            db = self.couch.connectDatabase('%s_requests' % site.lower())
            logger.info('queuing %s for %s' % (stage_data, site))
            db.commit(doc, timestamp='create_timestamp')

def do_options():
    op = OptionParser()
    op.add_option("-u", "--url",
              type="string", 
              action="store", 
              dest="couch", 
              help="CouchDB url. Default address 127.0.0.1:5984", 
              default="127.0.0.1:5984")
    
    op.add_option("-i", "--data",
              dest="data",
              default = [],
              action="append", 
              type="string", 
              help="The name of a dataset(s) or block(s) you wish to stage")
    
    op.add_option("-s", "--site",
              dest="site",
              default = [],
              action="append", 
              type="string", 
              help="The T1 site(s) you want to stage the data at")
    
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
    
    options, args = op.parse_args()
    
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger('StageManager')
    if options.verbose:
        logger.setLevel(logging.INFO)
    if options.debug:
        logger.setLevel(logging.DEBUG)

    logger.info('options: %s, args: %s' % (options, args))
    
    if len(options.site) > 0:
        for site in options.site:
            if not cmsname(site):
                logger.critical('%s is not a valid CMS name!' % site)
                sys.exit(101)
            if not site.startswith('T1'):
                logger.critical('%s is not a T1 site' % site)
                sys.exit(102)
    else:
        logger.critical('you need to provide a T1 site to stage at (-s option)')
        sys.exit(103)
    if len(options.data) == 0:
        logger.critical('you need to provide some data (dataset or block name) to stage')
        sys.exit(201)
    if options.verbose:
        logger.debug('Looks like some good input ya got yaself thar...')
        
    return options, args, logger


if __name__ == '__main__':
    options, args, logger = do_options()
    client = StageManagerClient(options.couch, logger)
    client.store_request(options.site, options.data)
    