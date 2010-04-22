'''
Poll a Couch instance for files to stage

@author: metson
'''
from optparse import OptionParser
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import cmsname
from WMCore.WMFactory import WMFactory
import sys
import time

def do_options():
    op = OptionParser()
    op.add_option("-u", "--local-url",
              dest="local", 
              help="Local CouchDB url. Default address 127.0.0.1:5984", 
              default="127.0.0.1:5984")
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

def sanitise_rows(rows):
    sanitised_data = []
    for i in rows:
        if 'doc' in i.keys():
            sanitised_data.append(i['doc'])
    return sanitised_data

opts, args = do_options()
server = CouchServer(opts.local)
db_name = opts.site.lower()
db = server.connectDatabase(db_name)
#Push views to the DB
views = {"_id":"_design/stagemanager",
         "language":"javascript",
         "views":{
                "retries":{"map":"function(doc) {  emit(doc.retry_count, 1);}",
                                "reduce":"function(key, values, rereduce){  return sum(values);}"},
                "backlog":{"map":"function(doc) {  emit(doc.state, doc.bytes);}",
                           "reduce":"function(key, values, rereduce){  return sum(values);}"},
                "file_state":{"map":"function(doc) {  emit(doc.state, 1);}",
                           "reduce":"function(key, values, rereduce){  return sum(values);}"}
                }
         }
db.commitOne(views)

#Set up tasty bi-directional replication
server.replicate('http://%s/%s' % (opts.remote,db_name), 
                 'http://%s/%s' % (opts.local,db_name), 
                 True, True)
server.replicate('http://%s/%s' % (opts.local,db_name), 
                'http://%s/%s' % (opts.remote,db_name), 
                 True, True)

factory = WMFactory('stager_factory', 'Agents.Stagers')
stager = factory.loadObject(opts.stager, args=[db, opts], listFlag = True)
db.compact(['stagemanager'])
#TODO: This should be a deamon
while True:
    #TODO: hit view for size of backlog, stop replication if over some limit
    data = db.loadView('stagemanager', 'file_state', {'reduce':False, 'include_docs':True})
    if len(data['rows']) > 0:
        data = sanitise_rows(data["rows"])
        if len(data) > 0:
            if opts.maxstage < 0:
                stager(data)
            else:
                c = 0
                lower = 0
                upper = opts.maxstage
                while c < len(data):
                    #TODO: subprocess?
                    stager(data[lower:upper])
                    c = upper
                    lower = upper - 1
                    upper += opts.maxstage 
                
    time.sleep(30)
    print 'compacting database'
    db.compact(['stagemanager'])