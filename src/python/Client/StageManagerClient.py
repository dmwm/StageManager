"""
Class which contains all code for client side
tools. Should be called by individual scripts
to invoke functionality
"""
import sys
import httplib
import time
from optparse import OptionParser
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Services.Requests import Requests
from WMCore.Wrappers import JsonWrapper

class StageManagerClient:
    def __init__(self, dburl, logger):
        self.couch = CouchServer(dburl)
        self.logger = logger

    def format_size(self, size):
        """
        Formats an argument in bytes into the nearest
        power-of-ten equivalent
        """
        divisors = [[1000000000000000.0, "PB"],
                    [1000000000000.0, "TB"],
                    [1000000000.0, "GB"],
                    [1000000.0, "MB"],
                    [1000.0, "kB"],
                    [1.0, "B"]]
        for i in divisors:
            val = size / i[0]
            if val > 1.0:
               return "%.2f %s" % (val, i[1])
        return "%.2f B" % size

    def format_progress(self, percent):
        """
        Returns a progress bar for the given range
        """
        done = int(percent)
        if done > 100: done = 100
        s = "["
        for i in range(int(done / 5.)): s += "-"
        for i in range(int(done /5.), 20): s += " "
        return s + "]"

    def query_requests(self, options):
        # Parse options
        self.site = options.site.lower()
        self.node = options.site
        to_show = {}
        if options.new:
            to_show['new'] = "New"
        if options.acquired:
            to_show['acquired'] = "Acquired"
        if options.done:
            to_show['done'] = "Done"
        if options.expired:
            to_show['expired'] = "Expired"


        # Set defaults if nothing asked for
        if len(to_show) == 0:
            to_show = {'new' : "New", 'acquired' : "Acquired"}

        # Run the queries
        results = {}
        for key in to_show:
            results[key] = self.query_request_type(key)

        # Create a list of all request IDs
        req_ids = []
        for key in to_show:
            for req in results[key]:
                req_ids.append(req['_id'])

        # Get the results for all keys 
        statistics = self.query_request_progress(req_ids) # Should be req_ids - not working...

        # We now have all results for the given key type, generate summaries
        summaries = {}
        for key in to_show:
            self.combine_request_information(results[key], statistics)
            summaries[key] = self.create_summary(results[key])

        # Finally (phew) print the information
        for key in to_show:
            print "%s (%s requests)" % (to_show[key], len(results[key]))
            if len(results[key]) > 0:
                self.display_summary(summaries[key])
                if not options.nodetail:
                    self.display_detail(results[key], options.data)

    def combine_request_information(self, requests, statistics):
        """
        Combines the request document with any recorded statistics
        """
        for req in requests:
            if req['_id'] in statistics.keys():
                req['done_files'] = statistics[req['_id']]['good']
                req['done_size'] = statistics[req['_id']]['staged_bytes']
            if req['total_files'] > 0:
                req['files_pc'] = req['done_files'] * 100.0 / req['total_files']
            if req['total_size'] > 0:
                req['size_pc'] = req['done_size'] * 100.0 / req['total_size']

    def format_epoch_time(self, t):
        return time.strftime("%d-%m-%Y %H:%M:%S", time.gmtime(t))

    def compute_rate(self, result):
        """
        Computes the rate of a request
        """
        start = result['accept_timestamp']
        end = time.time()
        if result['done_timestamp']:
            end = result['done_timestamp']
        delta = end - start
        if delta > 0:
            return result['done_size'] / delta

    def display_detail(self, results, showData):
        """
        Display detail about each request
        """
        i = 1
        for res in results:
            print "      %2d / %2d" % (i, len(results))
            i += 1
            if showData:
                print "         Data:  %s" % str(res['data'])
            if res['create_timestamp']:
                print "         Created:   %s" % self.format_epoch_time(res['create_timestamp'])
            if res['accept_timestamp']:
                print "         Acquired:  %s" % self.format_epoch_time(res['accept_timestamp'])
            if res['done_timestamp']:
                print "         Completed: %s" % self.format_epoch_time(res['done_timestamp'])
            if res['expired_timestamp']:
                print "         Expired:   %s" % self.format_epoch_time(res['expired_timestamp'])
            if res.has_key('due'):
                print "         Due by:    %s" % self.format_epoch_time(res['due'])
            print "         Files: %s (%s / %s)" % (self.format_progress(res['files_pc']),
                                                    res['done_files'], res['total_files'])
            print "         Size:  %s (%s / %s)" % (self.format_progress(res['size_pc']),
                                                    self.format_size(res['done_size']),
                                                    self.format_size(res['total_size']))
            if res['done_timestamp'] or res['accept_timestamp']:
                print "         Rate: %s/s" % self.format_size(self.compute_rate(res))

    def display_summary(self, summary):
        """
        Displays a request type summary
        """
        print "   Files: %s (%s / %s)" % (self.format_progress(summary['files_pc']),
                                        summary['done_files'], summary['total_files'])
        print "   Size:  %s (%s / %s)" % (self.format_progress(summary['size_pc']),
                                        self.format_size(summary['done_size']),
                                        self.format_size(summary['total_size']))

    def create_summary(key, results):
        """
        Summarises the query results
        """
        totalfiles, donefiles, totalsize, donesize = 0, 0, 0, 0
        for res in results:
            totalfiles += res['total_files']
            donefiles += res['done_files']
            totalsize += res['total_size']
            donesize += res['done_size']
        filespc, donepc = 0.0, 0.0
        if totalfiles > 0: filespc = donefiles * 100.0 / totalfiles
        if totalsize > 0: donepc = donesize * 100.0 / totalsize
        return {'total_files' : totalfiles, 'done_files' : donefiles,
                'total_size' : totalsize, 'done_size' : donesize,
                'files_pc' : filespc, 'size_pc' : donepc}

    def query_request_progress(self, requests):
        """
        Queries the progress of a request, or all requests, from the
        statistics DB
        """
        res = {}
        db = self.couch.connectDatabase('%s/statistics' % self.site)
        # Get requests, mark them as acquired
        data = {'rows':[]}
        try:
            for id in requests:
                data = db.loadView('statistics', 'request_progress', {'key':id, 'reduce':True, 'group_level':1})
                if len(data['rows']) > 0:
                    all_progress = self.sanitise_reduced_rows(data["rows"])
                    for single_progress in all_progress:
                        res[single_progress] = all_progress[single_progress]
        except httplib.HTTPException, he:
            self.handleHTTPExcept(he, 'could not retrieve request_progress view')
            sys.exit(1)
        return res

    def query_request_type(self, reqtype):
        """
        Gets all requests of given type, and fills in
        missing info
        """
        processed = []
        db = self.couch.connectDatabase('%s/requests' % self.site)
        # Get requests, mark them as acquired
        data = {'rows':[]}
        try:
            data = db.loadView('requests', 'request_state', {'reduce':False, 'include_docs':True, 'key':reqtype})
        except httplib.HTTPException, he:
            self.handleHTTPExcept(he, 'could not retrieve request_state view')
            sys.exit(1)
        if len(data['rows']) > 0:
            all_requests = self.sanitise_rows(data["rows"])
            for request in all_requests:
                if not request.has_key('total_files'):
                    request['total_files'] = 0
                if not request.has_key('total_size'):
                    request['total_size'] = 0
                request['done_files'] = 0
                request['done_size'] = 0
                request['files_pc'] = 0
                request['size_pc'] = 0
                if not request.has_key('accept_timestamp'):
                    request['accept_timestamp'] = None
                if not request.has_key('done_timestamp'):
                    request['done_timestamp'] = None
                if not request.has_key('expired_timestamp'):
                    request['expired_timestamp'] = None
                processed.append(request)
        return processed

    def store_request(self, sites = [], stage_data = [], due_date=False):
        self.logger.info('Requesting %s' % stage_data)
        doc = {'data': stage_data, 'state': 'new', 'create_timestamp' : time.time()}
        #If given a due_date we should respect that
        if due_date:
            doc['due'] = due_date
        
        self.logger.debug('request document: %s' % doc)
        
        for site in sites:
            # Check data is resident at site
            if self.check_resident(site, stage_data):
                db = self.couch.connectDatabase('%s/requests' % site.lower())
                self.logger.info('queuing %s for %s' % (stage_data, site))
                db.commit(doc)

    def check_resident(self, site, stage_data):
        """
        Checks that the requested data is resident on the site
        """
        goodToGo = True

        # Format site name
        locSite = site.replace('_Buffer', '_MSS')
        if not locSite.endswith('_MSS'):
            locSite += '_MSS'

        # Get block info from PhEDEx
        phedex = Requests(url='https://cmsweb.cern.ch', dict={'accept_type':'application/json'})
        for data in stage_data:
            if data.find('#') < 0:
                data = data + '*'
            self.logger.debug('Checking data residency for %s at %s' % (data, locSite))
            try:
                pdata = phedex.get('/phedex/datasvc/json/prod/blockreplicas', {
                                                           'dataset': data,
                                                           'node': locSite})[0]
            except httplib.HTTPException, he:
                self.handleHTTPExcept(he, 'HTTPException for block: %s node: %s' % (data, locSite))

            # Parse block info and check > 0 block exist
            try:
                if len(JsonWrapper.loads(pdata)['phedex']['block']) == 0:
                   goodToGo = False
                   self.logger.error('Block %s not resident at site %s' % (data, locSite))
            except:
                self.logger.debug('error loading json')
                goodToGo = False

        return goodToGo

    def handleHTTPExcept(self, he, message):
        """
        Some crude exception handling, just log the problem and move on...
        """
        self.logger.error(message)
        self.logger.info(he.status)
        self.logger.info(he.result)
        self.logger.info(he.reason)
        self.logger.info(he.message)

    def sanitise_reduced_rows(self, rows):
        sanitised_data = {}
        for i in rows:
            if 'key' in i.keys():
                sanitised_data[i['key']] = i['value']
        return sanitised_data

    def sanitise_rows(self, rows):
        """
        Quick n dirty method to make a list of docs
        """
        sanitised_data = []
        for i in rows:
            if 'doc' in i.keys():
                sanitised_data.append(i['doc'])
        return sanitised_data

