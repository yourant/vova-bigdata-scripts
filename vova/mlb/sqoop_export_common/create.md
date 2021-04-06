## 根据 [推荐流程与数据规范](https://confluence.gitvv.com/pages/viewpage.action?pageId=6124673) 开发的通用的召回数据导数脚本

- 数仓mlb库表由推荐组建表

- vova-bigdata-scripts 导数脚本

  - [mlb_vova_rec_i2i.sh](https://g.gitvv.com/bigdata/vova-bigdata-scripts/src/branch/master/vova/mlb/sqoop_export_common/mlb_vova_rec_i2i.sh)
  - [mlb_vova_rec_u2i.sh](https://g.gitvv.com/bigdata/vova-bigdata-scripts/src/branch/master/vova/mlb/sqoop_export_common/mlb_vova_rec_u2i.sh)
  - [mlb_vova_rec_q2i.sh](https://g.gitvv.com/bigdata/vova-bigdata-scripts/src/branch/master/vova/mlb/sqoop_export_common/mlb_vova_rec_q2i.sh)

  > 脚本手动执行时 第一个参数为 json 字符串

  ```shell
  sh mlb_vova_rec_i2i.sh '{"table_name":"mlb_vova_rec_i2i_match_support_a_d","dt":"2021-03-29"}'
  ```

- Azkaban flow:

  - [vova_mlb_rec_i2i_sqoop_export.flow](https://g.gitvv.com/bigdata/vova-bigdata-scripts/src/branch/master/vova/azkaban/mlb_export_d/vova_mlb_rec_i2i_sqoop_export.flow)
  - [vova_mlb_rec_u2i_sqoop_export.flow](https://g.gitvv.com/bigdata/vova-bigdata-scripts/src/branch/master/vova/azkaban/mlb_export_d/vova_mlb_rec_u2i_sqoop_export.flow)
  - [vova_mlb_rec_q2i_sqoop_export.flow](https://g.gitvv.com/bigdata/vova-bigdata-scripts/src/branch/master/vova/azkaban/mlb_export_d/vova_mlb_rec_q2i_sqoop_export.flow)

  > 不需要手动配置调度，通过消息弹起 flow

- job messager 配置:
  - 添加 召回类型与 Azkaban 中对应 flow 的关系 (已配置完成)

  ```shell
  curl -L -X POST 'http://ab81e133a11e611ebbee60ebf226a60d-1866282236.us-east-1.elb.amazonaws.com/vova/api/jobmss/upsert-job-flow' -H 'Content-Type: application/json' --data-raw '{
  data:{
    "jname" : "mlb_vova_i2i",
    "jfrom" : "mlb",
    "jto" : "data",
    "project_name" : "vova_mlb_export_d",  # Azkaban project name
    "flow_name" : "vova_mlb_rec_i2i_sqoop_export", # Azkaban flow name
    "knock_alias":"Andy.Zhang,Ethan.Zheng,Ted.wan,Juntao,kaicheng,Deyou,ruigong,Chuiyang,Ruohai" # 推荐组对应花名
    }
  }'
  
  curl -L -X POST 'http://ab81e133a11e611ebbee60ebf226a60d-1866282236.us-east-1.elb.amazonaws.com/vova/api/jobmss/upsert-job-flow' -H 'Content-Type: application/json' --data-raw '{
  data:{
    "jname" : "mlb_vova_u2i",
    "jfrom" : "mlb",
    "jto" : "data",
    "project_name" : "vova_mlb_export_d",  # Azkaban project name
    "flow_name" : "vova_mlb_rec_u2i_sqoop_export", # Azkaban flow name
    "knock_alias":"Andy.Zhang,Ethan.Zheng,Ted.wan,Juntao,kaicheng,Deyou,ruigong,Chuiyang,Ruohai" # 推荐组对应花名
    }
  }'
  
  
  curl -L -X POST 'http://ab81e133a11e611ebbee60ebf226a60d-1866282236.us-east-1.elb.amazonaws.com/vova/api/jobmss/upsert-job-flow' -H 'Content-Type: application/json' --data-raw '{
  data:{
    "jname" : "mlb_vova_q2i",
    "jfrom" : "mlb",
    "jto" : "data",
    "project_name" : "vova_mlb_export_d",  # Azkaban project name
    "flow_name" : "vova_mlb_rec_q2i_sqoop_export", # Azkaban flow name
    "knock_alias":"Andy.Zhang,Ethan.Zheng,Ted.wan,Juntao,kaicheng,Deyou,ruigong,Chuiyang,Ruohai" # 推荐组对应花名
    }
  }'
  ```

  - 推荐组同学需要发的消息

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










