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
        This is where the work is done. A list of files represented by a 
        dictionary are passed into the __call__ method and code is executed here
        to process each one.
        """
        start_time = str(datetime.datetime.now())
        staged, incomplete, failed = self.command(files)
        stage_end_time = str(datetime.datetime.now())
        msg = "%s files are staged, %s files are staging, %s files failed to stage"
        self.logger.info(msg % (len(staged), len(incomplete), len(failed)))
        self.mark_good(staged)
        self.mark_incomplete(incomplete)
        self.mark_failed(failed)
        #TODO: improve the stats dict, should include total size, timing etc. #24
        end_time = str(datetime.datetime.now())
        stats = {'good': staged, 
                 'failed': failed, 
                 'incomplete': incomplete,
                 'start_time': start_time,  
                 'stage_end_time': stage_end_time,
                 'end_time': end_time}

        self.record_stats(stats)
        
        self.queuedb.commit(viewlist=['stagequeue/file_state'])
        
    def command(self, files):
        """
        A null stager - files are never staged and just fail. This should be
        over ridden by subclasses. Return lists of staged, incomplete, failed 
        files as dictionaries. A file dict looks like:
        # {
        #   '_id': attrs.get('name'), 
        #   'bytes': int(attrs.get('bytes')),
        #   'checksum': {checksum.split(':')[0]: checksum.split(':')[1]},
        #   'state': 'new',
        #   'retry_count': [],
        #   'request_id': request_id
        # }
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
        stats_doc = {'start_time': stats['start_time'], 
                     'end_time': stats['end_time'], 
                     'stage_end_time': stats['stage_end_time']}
        
        results ={}
        # staged, failed and incomplete are a list of dicts, one dict per file, 
        # a file dict looks like:
        # {
        #   '_id': attrs.get('name'), 
        #   'bytes': int(attrs.get('bytes')),
        #   'checksum': {checksum.split(':')[0]: checksum.split(':')[1]},
        #   'state': 'new',
        #   'retry_count': [],
        #   'request_id': request_id
        # }
        # Build up per request stats for staged/failed/incomplete. Views will be
        # used to aggregate this information. 
        # TODO: #104
        for file in staged:  
          if file['request_id'] in results.keys():
            results[file['request_id']]['good'] += 1
            results[file['request_id']]['staged_bytes'] += file['bytes']
          else:
            results[file['request_id']] = {}
            results[file['request_id']]['timestamp'] = stats['stage_end_time']
            results[file['request_id']]['good'] = 1
            results[file['request_id']]['staged_bytes'] = file['bytes']
            results[file['request_id']]['failed'] = 0
            results[file['request_id']]['failed_bytes'] = 0
            results[file['request_id']]['incomplete'] = 0
            results[file['request_id']]['incomplete_bytes'] = 0
        # .. and for failed    
        for file in failed: # [] of {}'s, same as staged
          if file['request_id'] in results.keys():
            results[file['request_id']]['failed'] += 1
            results[file['request_id']]['failed_bytes'] += file['bytes']
          else:
            results[file['request_id']] = {}
            results[file['request_id']]['timestamp'] = stats['stage_end_time']
            results[file['request_id']]['failed'] = 1
            results[file['request_id']]['failed_bytes'] = file['bytes']
            results[file['request_id']]['good'] = 0
            results[file['request_id']]['staged_bytes'] = 0
            results[file['request_id']]['incomplete'] = 0
            results[file['request_id']]['incomplete_bytes'] = 0
        # and for incomplete    
        for file in incomplete: # [] of {}'s, same as staged
          if file['request_id'] in results.keys():
            results[file['request_id']]['incomplete'] += 1
            results[file['request_id']]['incomplete_bytes'] += file['bytes']
          else:
            results[file['request_id']] = {}
            results[file['request_id']]['timestamp'] = stats['stage_end_time']
            results[file['request_id']]['incomplete'] = 1
            results[file['request_id']]['incomplete_bytes'] = file['bytes']
            results[file['request_id']]['good'] = 0
            results[file['request_id']]['staged_bytes'] = 0
            results[file['request_id']]['failed'] = 0
            results[file['request_id']]['failed_bytes'] = 0

        stats_doc['results'] = results

        self.statsdb.commit(stats)
    
    def mark_good(self, files=[]):
        """
        Mark the list of files as staged
        """
        for i in files:
            self.queuedb.queueDelete(i, viewlist=['stagequeue/file_state'])        
    
    def mark_failed(self, files=[]):
        """
        Something failed for these files so increment the retry count
        """
        now = str(datetime.datetime.now())
        for i in files:
            i['state'] = 'acquired'
            i['retry_count'].append(now) 
            self.queuedb.queue(i, viewlist=['stagequeue/file_state'])
    
    def mark_incomplete(self, files=[]):
        """
        Mark the list of files as acquired
        """
        for i in files:
            i['state'] = 'acquired'
            self.queuedb.queue(i, viewlist=['stagequeue/file_state'])