import http.client
import json
import os
import sys
import time
import subprocess
def getResponse(ip,url):
    conn = http.client.HTTPConnection(ip, 8088)
    conn.request("GET", url, None, headers)
    return conn
if __name__ == "__main__":
    fileName=sys.argv[0]
    name=sys.argv[1]
    headers = {"accept": "application/json"}
    url = "http://ip-10-108-13-164.ec2.internal:8088/ws/v1/cluster/apps?state=RUNNING,ACCEPTED"
    url1 = "http://ip-10-108-3-0.ec2.internal:8088/ws/v1/cluster/apps?state=RUNNING,ACCEPTED"
    url2 = "http://ip-10-108-11-213.ec2.internal:8088/ws/v1/cluster/apps?state=RUNNING,ACCEPTED"
    ip ="ip-10-108-13-164.ec2.internal"
    ip1 ="ip-10-108-3-0.ec2.internal"
    ip2 ="ip-10-108-11-213.ec2.internal"
    arr = []
    try:
        conn=getResponse(ip,url)
        response = conn.getresponse()
        code = response.getcode()
        print(code)
        if(code!=200):
            conn = getResponse(ip1, url1)
            response = conn.getresponse()
            code = response.getcode()
            print(code)
            if(code !=200):
                conn = getResponse(ip2, url2)
                response = conn.getresponse()
                code = response.getcode()
                print(code)
        data = response.read()
        data = json.loads(data)
        apps = data['apps']['app']
        for app in apps:
            appName = app['name']
            arr.append(appName)
        if name not in arr:
            serverStart="/usr/lib/spark/sbin/start-thriftserver.sh --driver-memory 4g  --executor-memory 4G --executor-cores 1 --conf spark.dynamicAllocation.minExecutors=10  --conf spark.dynamicAllocation.maxExecutors=50  --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.network.timeout=300  --name {name} --master yarn --deploy-mode client --jars /mnt/mysql-connector-java-5.1.48.jar".format(name=name)
            p = subprocess.Popen("ps -ef | grep {name} | grep -v 'grep'".format(name=name), shell=True,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            print(p)
            for line in p.stdout.readlines():
                line=str(line)
                print(line)
                if fileName in line:
                    continue
                pid = line.split( )[1]
                closeCmd = 'kill -9 {pid}'.format(pid=pid)
                print(closeCmd)
                os.system(closeCmd)
            print(serverStart)
            os.system(serverStart)
            print(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time())))
    except Exception as e:
        print(e)
    finally:
        print(33)
        conn.close()

