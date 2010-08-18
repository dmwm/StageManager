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

from Client import StageManagerClient
from optparse import OptionParser
import logging
import sys
import time, datetime
from WMCore.Lexicon import cmsname

def do_options():
    op = OptionParser()
    op.add_option("-u", "--url",
              type="string", 
              action="store", 
              dest="couch", 
              help="CouchDB url. Default address http://127.0.0.1:5984", 
              default="http://127.0.0.1:5984")
    
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
    op.add_option("--due", 
              dest="due",
              default=False,
              help="Add a due date to the request. Date format is DD/MM/YYYY")
    options, args = op.parse_args()
    
    logging.basicConfig(level=logging.WARNING)
    logger = logging.getLogger('StageManager')
    if options.verbose:
        logger.setLevel(logging.INFO)
    if options.debug:
        logger.setLevel(logging.DEBUG)

    logger.info('options: %s, args: %s' % (options, args))
    
    if options.due:
        options.due = time.mktime(time.strptime(options.due, "%d/%m/%Y"))
    
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
    client = StageManagerClient.StageManagerClient(options.couch, logger)
    client.store_request(options.site, options.data, options.due)
   
 
