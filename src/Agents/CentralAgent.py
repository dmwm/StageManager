'''
The CentralAgent is used to perform routine maintenace (e.g. compaction) on the
central CouchDB instance. This is not needed at the sites because the 
StagerAgent triggers compaction as part of its life cycle.

Run as a cron??
'''
from optparse import OptionParser
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import cmsname

def do_options():
    op = OptionParser()
    op.add_option("-u", "--local-url",
              dest="dburl", 
              help="Local CouchDB url. Default address 127.0.0.1:5984", 
              default="127.0.0.1:5984")
    op.add_option("-s", "--site",
              dest="sites",
              default = [],
              action="append", 
              help="Name of the site the agent is running for", 
              default="T1_UK_RAL")
    op.add_option("-v", "--verbose",
              dest="verbose", 
              action="store_true",
              default=False, 
              help="Be more verbose")
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
    #TODO: persist options to local couch, pick them up 
    options, args = op.parse_args()
    if options.load:
        # Load options from the DB
        pass

    if options.verbose:
        print options, args

    if not cmsname(options.site):
        print '%s is not a valid CMS name!' % options.site
        sys.exit(101)
    elif not options.site.startswith('T1'):
        print '%s is not a T1 site' % options.site
        sys.exit(102)
    
    if options.persist:
        # Write the options to the config DB
        pass
    
    return options, args

opts, args = do_options()
server = CouchServer(opts.dburl)

for site in opts.sites:
    db_name = opts.site.lower()
    db = server.connectDatabase(db_name)
    
    print 'compacting database'
    db.compact(['stagemanager'])
#    time.sleep(3600)