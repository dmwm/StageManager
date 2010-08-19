import datetime

class Stager:
    def __init__(self, queuedb, statsdb, configdb, requestdb, config, logger):
        """
        A stager is set up with two instances of CMSCouchDB.Database (one 
        pointing at the stage queue the other at the statistics database) and is 
        responsible for marking the files it stages as staged/failed.
        """
        self.queuedb = queuedb
        self.statsdb = statsdb
        self.configdb = configdb
        self.requestdb = requestdb
        self.config = config
        self.save_config()
        # TODO: replace with Logger #23
        self.logger = logger
        
    def load_config(self):
        """
        Attempts to load the config from the DB
        """
        if self.configdb.documentExists("stager"):
            self.config = self.configdb.document("stager")

    def save_config(self):
        """
        Saves the config to the DB
        """
        # See if we have a config from the DB yet
        if self.config.has_key('_id') and self.config['_id'] == 'stager':
            self.configdb.commitOne(self.config)
        else:
            # First call of save_config without loading config first
            # See if there is a config in the DB, and overwrite params
            # rather than replace all - stager might have added information
            # that the user is not aware of on the command line.
            dbConfig = self.config
            if self.configdb.documentExists("stager"):
                dbConfig = self.configdb.document("stager")
                for key in self.config:
                    dbConfig[key] = self.config[key]
            else:
                dbConfig['_id'] = 'stager'
            # Only save if there are config options we care about
            if len(dbConfig) > 1:
                self.configdb.commitOne(dbConfig)

    def __call__(self, files=[]):
        """
        This is where the work is done. A list of files represented by a 
        dictionary are passed into the __call__ method and code is executed here
        to process each one.
        """
        # Load config from DB if present
        self.load_config()
        start_time = str(datetime.datetime.now())
        staged, incomplete, failed = self.command(files)
        self.save_config()
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

        # CURRENTLY BROKEN - COMMENTED OUT BY JJ FOR NOW
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
        for file in stats['good']:  
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
        for file in stats['failed']: # [] of {}'s, same as staged
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
        for file in stats['incomplete']: # [] of {}'s, same as staged
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
        # Track the number of files good in each request
        requestUpdates = {}
        for i in files:
            # Update good counts
            if requestUpdates.has_key(i['request_id']):
                requestUpdates[i['request_id']][0] += 1
                requestUpdates[i['request_id']][1] += i['bytes']
            else:
                requestUpdates[i['request_id']] = [1, i['bytes']]
            # Queue the good file for deletion from the queue
            self.queuedb.queueDelete(i, viewlist=['stagequeue/file_state'])        
    
        # Now update the requests
        for i in requestUpdates:
            doc = self.requestdb.document(i)
            if doc.has_key('done_files'):
                doc['done_files'] += requestUpdates[i][0]
                doc['done_size'] += requestUpdates[i][1]
            else:
                doc['done_files'] = requestUpdates[i][0]
                doc['done_size'] = requestUpdates[i][1]
            self.requestdb.queue(doc, viewlist=['requests/request_state'])
        if len(requestUpdates) > 0:
            self.requestdb.commit(viewlist=['requests/request_state'])

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
