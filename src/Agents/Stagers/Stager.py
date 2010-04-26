import datetime

class Stager:
    def __init__(self, db, logger):
        """
        A stager is set up with an instance of CMSCouchDB.Database and is 
        responsible for marking the files it stages as staged/failed.
        """
        self.couch = db
        # TODO: replace with Logger #23
        self.logger = logger
        
    def __call__(self, files=[]):
        """
        This is where the work is done. A list of files are passed into the 
        __call__ method and code is executed here to process each one.
        """
        staged, incomplete, failed = self.command(files)
        
        msg = "%s files are staged, %s files are staging, %s files failed to stage"
        self.logger.info(msg % (len(staged), len(incomplete), len(failed)))
        self.mark_good(staged)
        self.mark_incomplete(incomplete)
        self.mark_failed(failed)
        #TODO: calculate stats #24
        self.couch.commit(viewlist=['stagemanager/file_state'])
        
    def command(self, files):
        return [], [], files
    
    def mark_good(self, files=[]):
        """
        Mark the list of files as staged
        """
        for i in files:
            self.couch.queueDelete(i, viewlist=['stagemanager/file_state'])        
    
    def mark_failed(self, files=[]):
        """
        Something failed for these files so increment the retry count
        """
        now = str(datetime.datetime.now())
        for i in files:
            i['state'] = 'acquired'
            i['retry_count'].append(now) 
            self.couch.queue(i, viewlist=['stagemanager/file_state'])
    
    def mark_incomplete(self, files=[]):
        """
        Mark the list of files as acquired
        """
        for i in files:
            i['state'] = 'acquired'
            self.couch.queue(i, viewlist=['stagemanager/file_state'])