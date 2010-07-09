import subprocess
from Agents.Stagers.Stager import Stager

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
        Returns staged, incomplete, [] since failed is currently meaningless
        """
        
        staged = self.check_stage(files)
        incomplete = list(set(files) - set(staged))
        
        self.request_stage(incomplete)
        # Could check_stage here just in case the file comes online REAL fast
        # I think that's not such a hot idea, though, unless the frequency of 
        # the stager is very low.
        return staged, incomplete, []
        
    def check_stage(self, files):
        """
        Check that files are staged
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
        Request files are staged
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
        