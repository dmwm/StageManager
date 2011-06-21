'''
Queries the request database for information about requests
at a given site

py create_request.py -s T1_UK_RAL -u 192.168.179.134:5985 --new --acquired --done --expired

'''

from StageManager.Client import StageManagerClient
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
              help="CouchDB url. Default address http://127.0.0.1:5985", 
              default="http://127.0.0.1:5985")
    
    op.add_option("-s", "--site",
              dest="site",
              default="", 
              help="The T1 site you want to query")

    op.add_option("-n", "--new",
              dest="new",
              action="store_true",
              default=False,
              help="Show status of new requests")
    
    op.add_option("-a", "--acquired",
              dest="acquired",
              action="store_true",
              default=False,
              help="Show status of acquired requests")
    
    op.add_option("-d", "--done",
              dest="done",
              action="store_true",
              default=False,
              help="Show status of done requests")
    
    op.add_option("-e", "--expired",
              dest="expired",
              action="store_true",
              default=False,
              help="Show status of expired requests")
    
    op.add_option("-q", "--no-detail",
              dest="nodetail", 
              action="store_true",
              default=False, 
              help="Hide the pre-requests information and show only totals")

    op.add_option("-v", "--data",
              dest="data",
              action="store_true",
              default=False,
              help="Show the data requested against each status")

    options, args = op.parse_args()
    
    logger = logging.getLogger('StageManager')
   
    if options.site != "":
        if not cmsname(options.site):
            logger.critical('%s is not a valid CMS name!' % options.site)
            sys.exit(101)
        if not options.site.startswith('T1'):
            logger.critical('%s is not a T1 site' % options.site)
            sys.exit(102)
    else:
        logger.critical('you need to provide a T1 site to stage at (-s option)')
        sys.exit(103)
        
    return options, args, logger


if __name__ == '__main__':
    options, args, logger = do_options()
    client = StageManagerClient.StageManagerClient(options.couch, logger)
    client.query_requests(options)
   
 
