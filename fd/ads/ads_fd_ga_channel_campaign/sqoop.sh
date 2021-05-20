#bin/sh
table="ads_fd_ga_channel_campaign"
user="zhangyin"

base_path="/mnt/vova-bigdata-scripts/fd/ads"

if [ ! -n "$1" ]; then
  pt=$(date -d "-1 days" +"%Y-%m-%d")
else
  echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d "$1" +"%Y-%m-%d" >/dev/null
  if [[ $? -ne 0 ]]; then
    echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
    exit
  fi
  pt=$1
fi
echo "pt: ${pt}"

sql="
drop table if exists artemis.ads_fd_ga_channel_campaign_new;
drop table if exists artemis.ads_fd_ga_channel_campaign_pre;
"
mysql -h bd-warehouse-maxscale.gitvv.com -P 3311 -u market -pMyF4k2y9jJSv -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
CREATE TABLE IF NOT EXISTS artemis.ads_fd_ga_channel_campaign_new (
  order_id int(11) NOT NULL,
  pt varchar(16) NOT NULL DEFAULT '',
  domain_userid varchar(64) NOT NULL DEFAULT '0' comment '商品id',
  pre_event_time varchar(128) NOT NULL DEFAULT '',
  pre_ga_channel varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_mkt_source varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_campaign_name varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_campaign_id varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_adgroup_id varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_mkt_medium varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_mkt_term varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  PRIMARY KEY (order_id),
  KEY pt (pt) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS artemis.ads_fd_ga_channel_campaign (
  order_id int(11) NOT NULL,
  pt varchar(16) NOT NULL DEFAULT '',
  domain_userid varchar(64) NOT NULL DEFAULT '0' comment '商品id',
  pre_event_time varchar(128) NOT NULL DEFAULT '',
  pre_ga_channel varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_mkt_source varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_campaign_name varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_campaign_id varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_adgroup_id varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_mkt_medium varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  pre_mkt_term varchar(128) NOT NULL DEFAULT '0' comment '列表点击',
  PRIMARY KEY (order_id),
  KEY pt (pt) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"
mysql -h bd-warehouse-maxscale.gitvv.com -P 3311 -u market -pMyF4k2y9jJSv -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://bd-warehouse-maxscale.gitvv.com:3311/artemis \
--username market --password MyF4k2y9jJSv \
--m 1 \
--update-key "order_id" \
--update-mode allowinsert \
--table ads_fd_ga_channel_campaign_new \
--hcatalog-database ads \
--hcatalog-table ads_fd_ga_channel_campaign \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pt} \
--fields-terminated-by '\001'
if [ $? -ne 0 ]; then
  exit 1
fi

echo "----------开始rename-------"
mysql -h bd-warehouse-maxscale.gitvv.com -P 3311 -u market -pMyF4k2y9jJSv <<EOF
rename table artemis.ads_fd_ga_channel_campaign to artemis.ads_fd_ga_channel_campaign_pre,artemis.ads_fd_ga_channel_campaign_new to artemis.ads_fd_ga_channel_campaign;
EOF
echo "-------rename结束--------"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "table [$table] is finished !"