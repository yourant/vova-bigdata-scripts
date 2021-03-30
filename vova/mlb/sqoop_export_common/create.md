## 根据 [推荐流程与数据规范](https://confluence.gitvv.com/pages/viewpage.action?pageId=6124673) 开发的通用的召回数据导数脚本

- 数仓mlb库表由推荐组建表

- job messager 配置:
  - 添加 召回类型与 Azkaban 中对应 flow 的关系 

  ```shell
  curl -L -X POST 'http://ab81e133a11e611ebbee60ebf226a60d-1866282236.us-east-1.elb.amazonaws.com/vova/api/jobmss/upsert-job-flow' -H 'Content-Type: application/json' --data-raw '{
  data:{
    "jname" : "mlb_vova_i2i",
    "jfrom" : "mlb",
    "jto" : "data",
    "project_name" : "vova_mlb_export_d",  # Azkaban project name
    "flow_name" : "vova_mlb_rec_i2i_sqoop_export", # Azkaban flow name
    "knock_alias":"Andy.Zhang,Ethan.Zheng,Ted.wan,Juntao,kaicheng,deyou.shu,ruigong,Chuiyang,Ruohai" # 推荐组对应花名
    }
  }'
  
  curl -L -X POST 'http://ab81e133a11e611ebbee60ebf226a60d-1866282236.us-east-1.elb.amazonaws.com/vova/api/jobmss/upsert-job-flow' -H 'Content-Type: application/json' --data-raw '{
  data:{
    "jname" : "mlb_vova_u2i",
    "jfrom" : "mlb",
    "jto" : "data",
    "project_name" : "vova_mlb_export_d",  # Azkaban project name
    "flow_name" : "vova_mlb_rec_u2i_sqoop_export", # Azkaban flow name
    "knock_alias":"Andy.Zhang,Ethan.Zheng,Ted.wan,Juntao,kaicheng,deyou.shu,ruigong,Chuiyang,Ruohai" # 推荐组对应花名
    }
  }'
  
  
  curl -L -X POST 'http://ab81e133a11e611ebbee60ebf226a60d-1866282236.us-east-1.elb.amazonaws.com/vova/api/jobmss/upsert-job-flow' -H 'Content-Type: application/json' --data-raw '{
  data:{
    "jname" : "mlb_vova_q2i",
    "jfrom" : "mlb",
    "jto" : "data",
    "project_name" : "vova_mlb_export_d",  # Azkaban project name
    "flow_name" : "vova_mlb_rec_q2i_sqoop_export", # Azkaban flow name
    "knock_alias":"Andy.Zhang,Ethan.Zheng,Ted.wan,Juntao,kaicheng,deyou.shu,ruigong,Chuiyang,Ruohai" # 推荐组对应花名
    }
  }'
  ```

  - 推荐组同学发消息

  ```shell
  curl -L -X POST 'http://ab81e133a11e611ebbee60ebf226a60d-1866282236.us-east-1.elb.amazonaws.com/vova/api/jobmss/out' -H 'Content-Type: application/json' --data-raw '{
      "data":[
          { 
              "jname": "mlb_vova_i2i",
              "from": "mlb",
              "to": "data",
              "jstatus": "success",
              "jtype": "1D",
              "retry": "0",
              "freedoms":{"table_name":"i2i_test_a","dt":"2021-03-29"}
          }
      ]
  }'
  ```

  - 查看 Knock 消息， flow是否启动成功










