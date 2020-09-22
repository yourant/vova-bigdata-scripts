# 说明
该工程为大数据离线脚本工程，vova和fd的脚本分别放在各自的目录下，公共的脚本统一放在
common目录下
###重点介绍
判断依赖任务是否执行成功，执行以下命令
```
sh /mnt/vova-bigdata-scripts/common/judge_check.sh job_name
```

Demo1: flow内部job依赖
```
- name: first_job
    type: command
    config:
      command: sh /mnt/vova-bigdata-scripts/ads/first_job.sh

- name: second_job
    type: command
    config:
      command: sh /mnt/vova-bigdata-scripts/ads/second_job.sh
    dependsOn:
      - first_job
```

Demo2: 跨flow的job之间依赖

first_flow
```
- name: first_job
    type: command
    config:
      command: sh /mnt/vova-bigdata-scripts/ads/first_job.sh
```

second_flow
```
- name: judge_first_job
    type: command
    config:
      command: sh /mnt/vova-bigdata-scripts/common/judge_check.sh first_job
    dependsOn:
      - start_flag

- name: second_job
    type: command
    config:
      command: sh /mnt/vova-bigdata-scripts/ads/second_job.sh
    dependsOn:
      - judge_first_job
```

每次在固定机器上传脚本之后执行如下命令即可把脚本复制到另外两个节点
```
sh /mnt/vova-bigdata-scripts/common/sync.sh
```
