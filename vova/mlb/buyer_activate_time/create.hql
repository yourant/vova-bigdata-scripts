[8466]新用户冷启动用户属性召回下发策略优化
https://zt.gitvv.com/index.php?m=task&f=view&taskID=31403
任务描述
需求背景：
根据之前的ab实验结论，对新用户冷启动用户属性召回进行优化。区分用户类型和数据类型，对不同类型的用户应用不同的数据。
用户类型，区分新用户和老用户；
数据类型，区分低价优先和优质优先。

需求描述：
（1）区分新用户和老用户，
新用户：激活时间小于等于10天的用户；
老用户：激活时间大于10天的用户；
如果激活时间缺失，则默认为老用户。
将用户激活时间的数据导入到MySQL中，供服务同学调用。

（2）针对不同的下发方法进行ab实验，具体为：
s-和当前p组策略保持一致，采取低价优先（第一版）数据，无兜底策略；
t-采取低价优先（第一版）数据，增加兜底策略；
u-采取区分新老用户策略，对于新用户，出优质优先（第二版）数据，对老用户，采取低价优先（第一版）数据，增加兜底策略。

# 数据
取近180天有活跃的用户的激活时间
DROP TABLE  mlb.mlb_vova_buyer_activate_time_day180;
create table mlb.mlb_vova_buyer_activate_time_day180 (
buyer_id       bigint     COMMENT '用户id',
activate_time  timestamp  COMMENT '激活时间'
) COMMENT '近180天有pv的用户及激活时间' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

create table rec_recall.mlb_vova_buyer_activate_time(
\`id\`             int(11)   NOT NULL AUTO_INCREMENT COMMENT '自增主键',
\`buyer_id\`       bigint    NOT NULL COMMENT '用户id',
\`activate_time\`  timestamp NOT NULL COMMENT '用户当前设备激活时间',
PRIMARY KEY (\`id\`) USING BTREE,
UNIQUE KEY buyer_id (buyer_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='近180天有pv的用户及激活时间';


select * from mlb.mlb_vova_buyer_activate_time_day180 where buyer_id = 110498268;


