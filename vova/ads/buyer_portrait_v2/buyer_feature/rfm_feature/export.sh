#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
drop table if exists themis_logistics_report.ads_vova_rfm90_tag_new;
drop table if exists themis_logistics_report.ads_vova_rfm90_tag_pre;
CREATE TABLE IF NOT EXISTS themis_logistics_report.ads_vova_rfm90_tag_new(
  user_id int(11)  NOT NULL COMMENT '用户id',
  pm tinyint(1)    NOT NULL COMMENT 'RFM90_M,1:最近90天有消费，且最近90天内总消费金额大于等于70美元,2:最近90天有消费，且最近90天内总消费金额小于70美元,0:默认值',
  pf tinyint(1)    NOT NULL COMMENT 'RFM90_F,1:最近90天有消费，且最近90天内总消费天数大于等于3天,2:最近90天有消费，最近90天有消费，且最近90天内总消费天数小于3天,0:默认值',
  pr tinyint(1)    NOT NULL COMMENT 'RFM90_R,1:最近90天有消费，且最近一次消费日期在最近30天内（含30）,2:最近90天有消费，且最近一次消费日期不在最近30天内（不含30）,0:默认值',
  pn tinyint(1)    NOT NULL COMMENT 'RFM90_N,1:最近90天未消费，但90天之前消费过,2:用户激活时间小于等于7天，且用户从未消费过,3:用户激活时间大于7天小于等于30天，且用户从未消费过,4:用户激活时间大于30天，且用户从未消费过,0:默认值',
  pimp tinyint(1)  NOT NULL COMMENT 'RFM90_重要价值,1:RFM90_M+ ∩ RFM90_F+ ∩ RFM90_R+,2:RFM90_M- ∩ RFM90_F+ ∩ RFM90_R+,3:RFM90_M+ ∩ RFM90_F- ∩ RFM90_R+,4:RFM90_M- ∩ RFM90_F- ∩ RFM90_R+，5:RFM90_M+ ∩ RFM90_F- ∩ RFM90_R-,6:RFM90_M- ∩ RFM90_F- ∩ RFM90_R-,7:RFM90_M+ ∩ RFM90_F+ ∩ RFM90_R-,8:RFM90_M- ∩ RFM90_F+ ∩ RFM90_R-,0:默认值',
  update_time     datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='用户推送rfm标签';

CREATE TABLE IF NOT EXISTS themis_logistics_report.ads_vova_rfm90_tag(
  user_id int(11)  NOT NULL COMMENT '用户id',
  pm tinyint(1)    NOT NULL COMMENT 'RFM90_M,1:最近90天有消费，且最近90天内总消费金额大于等于70美元,2:最近90天有消费，且最近90天内总消费金额小于70美元,0:默认值',
  pf tinyint(1)    NOT NULL COMMENT 'RFM90_F,1:最近90天有消费，且最近90天内总消费天数大于等于3天,2:最近90天有消费，最近90天有消费，且最近90天内总消费天数小于3天,0:默认值',
  pr tinyint(1)    NOT NULL COMMENT 'RFM90_R,1:最近90天有消费，且最近一次消费日期在最近30天内（含30）,2:最近90天有消费，且最近一次消费日期不在最近30天内（不含30）,0:默认值',
  pn tinyint(1)    NOT NULL COMMENT 'RFM90_N,1:最近90天未消费，但90天之前消费过,2:用户激活时间小于等于7天，且用户从未消费过,3:用户激活时间大于7天小于等于30天，且用户从未消费过,4:用户激活时间大于30天，且用户从未消费过,0:默认值',
  pimp tinyint(1)  NOT NULL COMMENT 'RFM90_重要价值,1:RFM90_M+ ∩ RFM90_F+ ∩ RFM90_R+,2:RFM90_M- ∩ RFM90_F+ ∩ RFM90_R+,3:RFM90_M+ ∩ RFM90_F- ∩ RFM90_R+,4:RFM90_M- ∩ RFM90_F- ∩ RFM90_R+，5:RFM90_M+ ∩ RFM90_F- ∩ RFM90_R-,6:RFM90_M- ∩ RFM90_F- ∩ RFM90_R-,7:RFM90_M+ ∩ RFM90_F+ ∩ RFM90_R-,8:RFM90_M- ∩ RFM90_F+ ∩ RFM90_R-,0:默认值',
  update_time     datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT COMMENT='用户推送rfm标签';
"

mysql -h db-logistics-w.gitvv.com -u vvreport20210517 -pthuy*at1OhG1eiyoh8she -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8192 \
-Dmapreduce.reduce.memory.mb=8192 \
-Dsqoop.export.records.per.statement=100 \
--connect 'jdbc:mysql://db-logistics-w.gitvv.com:3306/themis_logistics_report?disableMariaDbDriver' \
--username vvreport20210517 --password thuy*at1OhG1eiyoh8she \
--table ads_vova_rfm90_tag_new \
--m 20 \
--hcatalog-database ads \
--hcatalog-table ads_vova_rfm90_tag \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pt} \
--columns  user_id,pm,pf,pr,pn,pimp \
--fields-terminated-by '\001'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

mysql -h db-logistics-w.gitvv.com -u vvreport20210517 -pthuy*at1OhG1eiyoh8she <<EOF
rename table themis_logistics_report.ads_vova_rfm90_tag to themis_logistics_report.ads_vova_rfm90_tag_pre,themis_logistics_report.ads_vova_rfm90_tag_new to themis_logistics_report.ads_vova_rfm90_tag;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi