#!/bin/bash
scp -i /home/hadoop/.ssh/id_rsa -r /mnt/vova-bigdata-scripts/ hadoop@ip-10-108-5-194.ec2.internal:/mnt/
scp -i /home/hadoop/.ssh/id_rsa -r /mnt/vova-bigdata-scripts/ hadoop@ip-10-108-0-51.ec2.internal:/mnt/