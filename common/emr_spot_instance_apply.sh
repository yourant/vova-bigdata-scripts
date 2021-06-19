 #!/bin/bash
r16_instance_count=$1
if [ ! -n "$2" ];then
r16_innstance_group_id=ig-1BL6OSXT1ZXBF
fi
#exit 1
echo "start to apply spot instance"
aws emr modify-instance-groups --instance-groups InstanceGroupId=${r16_innstance_group_id},InstanceCount=${r16_instance_count}

#如果脚本失败，则报错
if [ $? -ne 0 ];then
   echo "emr spot apply error"
   exit 1
fi
