 #!/bin/bash
r24_instance_count=$1
r12_instance_count=$2
r24_innstance_group_id=$3
if [ ! -n "$3" ];then
r24_innstance_group_id=ig-2H738Z0BIV7A2
fi
r12_innstance_group_id=$4
if [ ! -n "$4" ];then
r12_innstance_group_id=ig-38SQQ52BBT4NJ
fi
#exit 1
echo "start to apply spot instance"
aws emr modify-instance-groups --instance-groups InstanceGroupId=${r24_innstance_group_id},InstanceCount=${r24_instance_count}
aws emr modify-instance-groups --instance-groups InstanceGroupId=${r12_innstance_group_id},InstanceCount=${r12_instance_count}

#如果脚本失败，则报错
if [ $? -ne 0 ];then
   echo "emr spot apply error"
   exit 1
fi
