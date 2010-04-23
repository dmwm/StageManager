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
import httplib
import urllib
import logging
import os
import pwd
import sys
from WMCore.Services.Requests import Requests
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import cmsname
from WMCore.Wrappers import JsonWrapper
from xml.dom import minidom

def do_options():
    op = OptionParser()
    op.add_option("-u", "--url",
                  type="string", 
                  action="store", 
                  dest="couch", 
                  help="CouchDB url. Default address 127.0.0.1:5984", 
                  default="127.0.0.1:5984")
    
    op.add_option("-d", "--data",
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
    
    options, args = op.parse_args()
    if options.verbose:
        print options, args
    if len(options.site) > 0:
        for site in options.site:
            if not cmsname(site):
                print '%s is not a valid CMS name!' % site
                sys.exit(101)
            if not site.startswith('T1'):
                print '%s is not a T1 site' % site
                sys.exit(102)
    else:
        print 'you need to provide a T1 site to stage at (-s option)'
        sys.exit(103)
    if len(options.data) == 0:
        print 'you need to provide some data (dataset or block name) to stage'
        sys.exit(201)
    if options.verbose:
        print 'Looks like some good input ya got yaself thar...'
    return options, args

def process_files(options):
    sites = options.site
    stage_data = options.data
    # Need to clean up the input a bit
    for s in sites:
        # Only deal with MSS nodes
        idx = sites.index(s)
        s.replace('_Buffer', '_MSS')
        if not s.endswith('_MSS'):
            s += '_MSS'
        sites[idx] = s
    for d in stage_data:
        # Need to pass in blocks
        if d.find('#') < 0:
            stage_data[stage_data.index(d)] = d + '*'
    
    phedex = Requests(url='cmsweb.cern.ch', dict={'accept_type':'text/xml'})

    couch = CouchServer(options.couch)
    #TODO: subprocess here per site? #26
    for node in sites:
        #TODO: use logger #23
        if options.verbose:
            print 'Creating stage-in requests for %s' % node
        db = couch.connectDatabase(node.replace('_MSS', '').lower())
        try:
            data = phedex.get('/phedex/datasvc/xml/prod/filereplicas', {
                                                    'block':stage_data, 
                                                    'node': node})[0]
        except httplib.HTTPException, he:
            print 'HTTPException for block: %s node: %s' % (data, node)
            print he.status
            print he.result
            print he.reason
            print he.message
            
        #<file checksum='cksum:2470571517' bytes='1501610356' 
        #name='/store/mc/Summer09/MinBias900GeV/AODSIM/MC_31X_V3_AODSIM-v1/0021/F0C49EA2-FA88-DE11-B886-003048341A94.root' 
        #id='29451522' origin_node='T2_US_Wisconsin' time_create='1250711698.34438'><replica group='DataOps' node_id='19' se='srm-cms.gridpp.rl.ac.uk' custodial='y' subscribed='y' node='T1_UK_RAL_MSS' time_create=''/></file>
        
        #TODO: coroutine here #26
        dom = minidom.parseString(data)
        for stgfile in dom.getElementsByTagName('file'):
            checksum = stgfile.getAttribute('checksum')
            f ={'_id': stgfile.getAttribute('name'),
                'bytes': int(stgfile.getAttribute('bytes')),
                'checksum': {checksum.split(':')[0]: checksum.split(':')[1]},
                'state': 'new',
                'retry_count': []
                }
            db.queue(f, timestamp = True)
        db.commit()
        

if __name__ == '__main__':
    opt, args = do_options()
    process_files(opt)
    