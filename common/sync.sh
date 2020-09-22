#!/bin/bash
scp -i /home/hadoop/hello-ssh.pem -r /mnt/vova-bd-scripts/ hadoop@ip-192-168-65-239.ec2.internal:/mnt/
scp -i /home/hadoop/hello-ssh.pem -r /mnt/vova-bd-scripts/ hadoop@ip-192-168-69-27.ec2.internal:/mnt/