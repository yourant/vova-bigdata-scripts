create external TABLE ods_vova_ext.ods_vova_vvqueue_user_nps_arc
(
json_str                string      COMMENT 'json_str'
) COMMENT 'nps数据' PARTITIONED BY (pt STRING)
LOCATION "s3://bigdata-offline/warehouse/pdb/vova/vvqueue/vvqueue-user_nps"
;

drop table if exists ods_vova_ext.ods_vova_vvqueue_user_nps;
CREATE EXTERNAL TABLE IF NOT EXISTS ods_vova_ext.ods_vova_vvqueue_user_nps
(
    user_email           string,
    user_id              bigint,
    reasons              string,
    reason_text          string,
    add_time             timestamp,
    score                int
) COMMENT 'nps数据'
    PARTITIONED BY (pt string);