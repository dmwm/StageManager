import subprocess
import os
from Agents.Stagers.Stager import Stager

class CASTORStager(Stager):
    '''
    A generic CASTOR stager. Needs to be called with the options:
       "svcClass=SVCCLASS,castorRoot=/castor/ads.rl.ac.uk/prod/cms"
    '''
    STAGER_GET = 'stager_get -S %s -M'
    STAGER_QRY = 'stager_qry -S %s -M'
    
    def command(self, files):
        """
        Passes files to stager_qry / stager_get. Will attempt as many files
        on the command line as are passed, so appropriate throttling should
        be set in the calling agent so as to not kill CASTOR / the shell
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
        
        files = [self.config['castorRoot'] + a for a in files]
        command = '%s %s' % (self.STAGER_QRY % self.config['svcClass'], ' -M '.join(files))
        proc.stdin.write(command)
        stdout, stderr =  proc.communicate()
        rc = proc.returncode
        responses = stdout.split('\n')
        staged = []
        for response in responses:
            if response.find("STAGED") > 0:
                staged.append(response.split()[0].replace(self.config['castorRoot'], ""))
        return staged
    
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
        
        files = [self.config['castorRoot'] + a for a in files]
        command = '%s %s' % (self.STAGER_GET % self.config['svcClass'], ' -M '.join(files))
        proc.stdin.write(command)
        stdout, stderr =  proc.communicate()
        rc = proc.returncode
        
