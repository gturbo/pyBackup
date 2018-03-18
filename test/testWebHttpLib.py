import httplib

conn = httplib.HTTPConnection(host="sriopmgta0101.recette.local", port=50070, )
conn.request("GET", "/webhdfs/v1/temp?user.name=hdfs&op=GETFILESTATUS")
resp = conn.getresponse()
print("resp: {0}".format(resp.status))
if resp.status == 200:
    print(resp.read())
else:
    raise Exception("unable to request {0} status {1}".format(resp.msg, resp.status))
