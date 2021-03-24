import os
import requests

knockUrl = "https://eventhub.gitvv.com/api/v1/notifications/knock?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6InZ2ZXZlbnRodWIiLCJwYXNzd29yZCI6InBlNG9vbTdPanVyPWVlL2c4dyIsImp0aSI6Im1pY2ZtcFVqYUM4bUlRT3kiLCJpc3MiOiJ2dkV2ZW50SHViIn0.xWIiN25VqH7ppJ8A_nZ2KbkeSybr3PTzDy0-SEzWLbE"
knockUser = '["andy.zhang"]'
headers = {"accept": "application/json"}

def knock():
    message = "yarn上TASK node label 标签丢失"
    knockJson = '{"alias":%s,"messageType":%d,"message":"%s"}' % (knockUser, 3, message)
    r = requests.post(knockUrl, data=knockJson.encode(), headers=headers)
    print(r.text)
    r.close()

if __name__ == '__main__':
    command = 'yarn cluster --list-node-labels'
    r = os.popen(command)
    info = r.readlines()
    node=''
    for line in info:
        if("Labels" in line):
            node=line+''
    print('--------------------------')
    print(node)

    if("TASK" in node):
        print("----TASK标签存在-----")
    else:
        print("-------TASK标签丢失-----------")
        knock()
        os.system('yarn rmadmin -addToClusterNodeLabels "TASK(exclusive=false)" ')
