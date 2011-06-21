import subprocess
from StageManager.Agents.Stagers.Stager import Stager

class FNALStager(Stager):
    '''
    A stager for fermi lab, using the script provided by Paul Rossman.
    '''
    # Depends on #29 to not require a hard coded path
    STAGER_COMMAND = 'FilePrestage'
    
    def command(self, files):
        """
        In this stager files are passed to a script one by one. The script used
        is http://cmssw.cvs.cern.ch/cgi-bin/cmssw.cgi/COMP/SITECONF/T1_US_FNAL/PhEDEx/FilePrestage?view=markup
        Returns staged, incomplete, [] since failed is currently meaningless.
        
        files is a list of dictionaries describing the files to stage. The '_id'
        key contains the lfn, which is what we want to use here. 
        """
        # Since files is a list of dicts, and we want a list of lfn's use a map
        # function to pull the lfn out of the dictionary
        def lfn_map(file_dict):
            return file_dict['_id']
        
        in_lfn_list = map(lfn_map, files)
        
        # We now check to see if those lfn's are staged 
        staged = self.check_stage(in_lfn_list)
        # and assume all files that aren't returned are incomplete
        incomplete = list(set(in_lfn_list) - set(staged))
        # and so ask for them to be staged
        self.request_stage(incomplete)
        
        # command() needs to return three lists of dicts so build that here with
        # a filter function.
        #
        # First make a list to hold files that weren't staged, and bind the 
        # function to save some time. 
        incomplete_files = []
        record_incomplete = incomplete_files.append
        # Define a filter function that returns true if the file dictionary has 
        # an lfn we know to be staged, and record the file dictionaries that 
        # aren't staged into our incomplete_files list. 
        def staged_files_filter(file_dict):
            if file_dict['_id'] in staged:
                return True
            else:
                record_incomplete(file_dict)
        
        # now return the filtered files (the ones that are staged), the ones 
        # that didn't make the cut and an empty list because failed is 
        # meaningless here.
        return filter(staged_files_filter, files), incomplete_files, []
        
    def check_stage(self, files):
        """
        Check that files are staged, returns a list of lfn's that were staged.
        """
        proc = subprocess.Popen(
                ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE,
            )
        
        command = '%s -q %s' % (STAGER_COMMAND, ' '.join(files))
        proc.stdin.write(command)
        stdout, stderr =  proc.communicate()
        rc = proc.returncode
        return stdout.split('\n')
    
    def request_stage(self, files):
        """
        Request files are staged, returns nothing.
        """
        proc = subprocess.Popen(
                ["/bin/bash"], shell=True, cwd=os.environ['PWD'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE,
            )
        
        command = '%s -s %s' % (STAGER_COMMAND, ' '.join(files))
        proc.stdin.write(command)
        stdout, stderr =  proc.communicate()
        rc = proc.returncode
        
