#!/bin/bash
scp -i /home/hadoop/.ssh/id_rsa -r /mnt/vova-bigdata-scripts/ hadoop@ip-10-108-3-7.ec2.internal:/mnt/
scp -i /home/hadoop/.ssh/id_rsa -r /mnt/vova-bigdata-scripts/ hadoop@ip-10-108-9-243.ec2.internal:/mnt/