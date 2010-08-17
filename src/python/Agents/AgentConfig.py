# This data structure defines the available command line options which can
# be passed to the Stager Agent upon startup. If the persist option is set
# to True, the option will be saved to the configuration database.
#
# The loading algorithm is defined in the stager agent, but the following
# prescedent rules apply:
#
# 1. Load existing configuration from database (if present)
# 2. Update configuration with any parameters specified on the command line
# 3. For any parameters which are not loaded above, use the defaults
#
# These rules allow for a crude versioning system; if a new option is added,
# the loading from DB will not fail and the new default will be used (if not
# specified on the command line)
#
 
defaultOptions = {
    "-u" : {"long" : "--local-url",
            "opts" :
              {"dest" : "localdb",
               "help" : "Local CouchDB url. Default address http://127.0.0.1:5984",
               "default" : "http://127.0.0.1:5984"
              },
            "persist" : False
           },
    "-r" : {"long" : "--remote-url",
            "opts" :
              {"dest" : "remotedb",
               "help" : "Remote CouchDB url. Default address cmsweb.cern.ch/stager",
               "default" : "http://cmsweb.cern.ch/stager"
              },
            "persist" : True
           },
    "-s" : {"long" : "--site",
             "opts" :
               {"dest" : "site",
                "help" : "Name of the site the agent is running for",
                "default" : None
               },
             "persist" : False
            },
    "-v" : {"long" : "--verbose",
             "opts" :
               {"dest" : "verbose",
                "action" : "store_true",
                "default" : False,
                "help" : "Be more verbose"
               },
            "persist" : False
           },
    "-d" : {"long" : "--debug",
            "opts" :
               {"dest" : "debug",
                "action" : "store_true",
                "default" : False,
                "help" : "Be extremely verbose - print debugging statements"
               },
            "persist" : False
           },
    "-m" : {"long" : "--max-stage",
            "opts" :
               {"dest" : "maxstage",
                "default" : 0,
                "type" : "int",
                "metavar" : "NUM",
                "help" : "Send the stager NUM requests at a time"
               },
            "persist" : True  
           },
    "-w" : {"long" : "--wait",
            "opts" :
               {"dest" : "waittime",
                "default" : 300,
                "type" : "int",
                "metavar" : "SECONDS",
                "help" : "Time to wait if no work to be done"
               },
            "persist" : True
           },
    "-p" : {"long" : "--stager-plugin",
            "opts" :
               {"dest" : "stagerplugin",
                "default" : "FakeStager",
                "help" : "Name of stager plugin"
               },
            "persist" : True
           },
    "-o" : {"long" : "--stager-options",
            "opts" :
               {"dest" : "stageroptions",
                "default" : "",
                "help" : "Options to pass to stager in form \"key1=val1,key2=val2\""
               },
            "persist" : False
           }
}

