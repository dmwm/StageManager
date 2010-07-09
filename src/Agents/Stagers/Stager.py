import datetime

class Stager:
    def __init__(self, queuedb, statsdb, logger):
        """
        A stager is set up with two instances of CMSCouchDB.Database (one 
        pointing at the stage queue the other at the statistics database) and is 
        responsible for marking the files it stages as staged/failed.
        """
        self.queuedb = queuedb
        self.statsdb = statsdb
        # TODO: replace with Logger #23
        self.logger = logger
        
    def __call__(self, files=[]):
        """
        This is where the work is done. A list of files are passed into the 
        __call__ method and code is executed here to process each one.
        """
        start_time = str(datetime.datetime.now())
        staged, incomplete, failed = self.command(files)
        #TODO: Do we want to know the request id? It's not that meaningful given
        # the asynchronous nature of populating the file list and the calls to 
        # the stager __call__ method 
        msg = "%s files are staged, %s files are staging, %s files failed to stage"
        self.logger.info(msg % (len(staged), len(incomplete), len(failed)))
        self.mark_good(staged)
        self.mark_incomplete(incomplete)
        self.mark_failed(failed)
        #TODO: improve the stats dict, should include total size, timing etc. #24
        end_time = str(datetime.datetime.now())
        stats = {'good': len(staged), 'failed': len(failed),
                 'start_time': start_time, 'end_time': end_time}
        self.record_stats(stats)
        
        self.queuedb.commit(viewlist=['stagemanager/file_state'])
        
    def command(self, files):
        """
        A null stager - files are never staged and just fail. This should be
        over ridden by subclasses. Return staged, incomplete, failed files.
            staged: file is on disk
            incomplete: file has been requested but is not on the disk pool
            failed: file could not be staged - may not be supported by MSS
        """
        
        return [], [], files
    
    def record_stats(self, stats):
        """
        Push a stats dict into Couch referencing the request (to be replicated
        off site)
        """
        #TODO: refresh statistics views #24
        self.statsdb.commit(stats)
    
    def mark_good(self, files=[]):
        """
        Mark the list of files as staged
        """
        for i in files:
            self.queuedb.queueDelete(i, viewlist=['stagemanager/file_state'])        
    
    def mark_failed(self, files=[]):
        """
        Something failed for these files so increment the retry count
        """
        now = str(datetime.datetime.now())
        for i in files:
            i['state'] = 'acquired'
            i['retry_count'].append(now) 
            self.queuedb.queue(i, viewlist=['stagemanager/file_state'])
    
    def mark_incomplete(self, files=[]):
        """
        Mark the list of files as acquired
        """
        for i in files:
            i['state'] = 'acquired'
            self.queuedb.queue(i, viewlist=['stagemanager/file_state'])