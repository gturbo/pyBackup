from urllib2 import Request, urlopen
from urllib2 import URLError

req = Request("http://sriopmgta0101.recette.local:50070/webhdfs/v1/temp?user.name=hdfs&op=GETFILESTATUS")
try:
    response = urlopen(req)
except URLError as e:
    if hasattr(e, 'reason'):
        print('received status: {0}'.format(e.))
        print('Reason: ', e.reason)
    elif hasattr(e, 'code'):
        print('The server couldn\'t fulfill the request.')
        print('Error code: ', e.code)
else:
    pass
