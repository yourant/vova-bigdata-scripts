#bin/sh
table="ads_fd_base_snowplow"
user="zhubao"

base_path="/mnt/vova-bigdata-scripts/fd/ads"

if [ ! -n "$1" ]; then
  pt=$(date -d "-0 days" +"%Y-%m-%d")
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
drop table if exists artemis.ads_fd_base_snowplow_new;
drop table if exists artemis.ads_fd_base_snowplow_pre;
"
mysql -h bd-warehouse-maxscale.gitvv.com -P 3311 -u market -pMyF4k2y9jJSv -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
CREATE TABLE IF NOT EXISTS artemis.ads_fd_base_snowplow_new
(
    event_fingerprint       varchar(128),
    dt                      varchar(64),
    country                 varchar(128),
    domain_userid           varchar(64),
    useragent               varchar(350),

    element_name            varchar(128),
    adgroup_id              varchar(128),
    ads_type                varchar(128),
    campaign_id             varchar(128),
    adset_id                varchar(128),
    pt                      varchar(16),
    KEY pt (pt) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS artemis.ads_fd_base_snowplow
(
    event_fingerprint       varchar(128),
    dt                      varchar(64),
    country                 varchar(128),
    domain_userid           varchar(64),
    useragent               varchar(350),

    element_name            varchar(128),
    adgroup_id              varchar(128),
    ads_type                varchar(128),
    campaign_id             varchar(128),
    adset_id                varchar(128),
    pt                      varchar(16),
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
--m 6 \
--table ads_fd_base_snowplow \
--hcatalog-database ads \
--hcatalog-table ads_fd_base_snowplow \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pt} \
--fields-terminated-by '\001'
if [ $? -ne 0 ]; then
  exit 1
fi

echo "----------开始insert临时表ads_fd_base_snowplow_notice-------"
mysql -h bd-warehouse-maxscale.gitvv.com -P 3311 -u market -pMyF4k2y9jJSv <<EOF
rename table artemis.ads_fd_base_snowplow to artemis.ads_fd_base_snowplow_pre,artemis.ads_fd_base_snowplow_new to artemis.ads_fd_base_snowplow;
EOF
echo "-------insert结束--------"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "table [$table] is finished !"