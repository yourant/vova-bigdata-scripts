 #!/bin/bash
instance_count=$1
innstance_group_id=$2
if [ ! -n "$2" ];then
innstance_group_id=ig-139IKLQFFE9ZX
fi
#exit 1
echo "start to apply spot instance"
aws emr modify-instance-groups --instance-groups InstanceGroupId=${innstance_group_id},InstanceCount=${instance_count}

#如果脚本失败，则报错
if [ $? -ne 0 ];then
   echo "emr spot apply error"
   exit 1
fi
