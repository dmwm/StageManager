from Agents.Stagers.Stager import Stager

class PassThroughStager(object):
    '''
    A stager that just returns that everything it's been given has been staged.
    For testing only.
    '''
    def command(self, files):
        """
        All files stage immediately
        """
        return files, [], []