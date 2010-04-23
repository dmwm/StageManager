from Agent.Stagers.Stager import Stager
import random

class FakeStager(Stager):
    def command(self, files):
        """
        In this stager a random population is taken as being successfully staged
        incomplete and the rest marked as a failure.
        """
        
        staged = random.sample(files, random.randint(0, len(files)))
        for f in staged:
            files.remove(f)
        incomplete = random.sample(files, random.randint(0, len(files)))
        for f in incomplete:
            files.remove(f)
                
        return staged, incomplete, files
    