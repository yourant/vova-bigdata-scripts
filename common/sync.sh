#!/bin/bash
scp -i /home/hadoop/vova-bd-ssh.pem -r /mnt/vova-bigdata-scripts/ hadoop@ip-10-108-15-103.ec2.internal:/mnt/
scp -i /home/hadoop/vova-bd-ssh.pem -r /mnt/vova-bigdata-scripts/ hadoop@ip-10-108-7-65.ec2.internal:/mnt/