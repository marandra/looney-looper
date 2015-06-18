import sys
import imp

# get list of files in plugin dir
pluginfiles = ['./EMBOSS.py']
for plugin in pluginfiles:
    module = imp.load_source('plug', plugin)
    print plugin
    try:
        instance = module.create()
    except:
        print "No Create Function defined in module %s" % plugin
        sys.exit(1)
    print instance.second
    print instance.minute
    print instance.hour
    print instance.day_of_week
    print instance.stable_second
    print instance.stable_minute
    print instance.stable_hour
    print instance.stable_day_of_week
    print instance.person	
    print instance.email
    print instance.check_update_stable('.', 'updateme')

# emails for users...
