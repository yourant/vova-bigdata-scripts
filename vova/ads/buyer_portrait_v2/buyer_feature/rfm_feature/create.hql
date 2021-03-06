-- 用户推送rfm标签
-- 参考需求链接：https://zt.gitvv.com/index.php?m=task&f=view&taskID=34284

--注释超长写上面
-- RFM90_M,1:最近90天有消费，且最近90天内总消费金额大于等于70美元,2:最近90天有消费，且最近90天内总消费金额小于70美元,0:默认值
-- RFM90_F,1:最近90天有消费，且最近90天内总消费天数大于等于3天,2:最近90天有消费，最近90天有消费，且最近90天内总消费天数小于3天,0:默认值
-- RFM90_R,1:最近90天有消费，且最近一次消费日期在最近30天内（含30）,2:最近90天有消费，且最近一次消费日期不在最近30天内（不含30）,0:默认值
-- RFM90_N,1:最近90天未消费，但90天之前消费过,2:用户激活时间小于等于7天，且用户从未消费过,3:用户激活时间大于7天小于等于30天，且用户从未消费过,4:用户激活时间大于30天，且用户从未消费过,0:默认值
-- RFM90_重要价值,1:RFM90_M+ ∩ RFM90_F+ ∩ RFM90_R+,2:RFM90_M- ∩ RFM90_F+ ∩ RFM90_R+,3:RFM90_M+ ∩ RFM90_F- ∩ RFM90_R+,4:RFM90_M- ∩ RFM90_F- ∩ RFM90_R+，5:RFM90_M+ ∩ RFM90_F- ∩ RFM90_R-,6:RFM90_M- ∩ RFM90_F- ∩ RFM90_R-,7:RFM90_M+ ∩ RFM90_F+ ∩ RFM90_R-,8:RFM90_M- ∩ RFM90_F+ ∩ RFM90_R-,0:默认值

CREATE external TABLE `ads`.`ads_vova_rfm90_tag`(
  `user_id`     bigint    COMMENT 'd_买家id',
  `pm`          int       COMMENT 'RFM90_M,1:最近90天有消费，且最近90天内总消费金额大于等于70美元,2:最近90天有消费，且最近90天内总消费金额小于70美元,0:默认值',
  `pf`          int       COMMENT 'RFM90_F,1:最近90天有消费，且最近90天内总消费天数大于等于3天,2:最近90天有消费，最近90天有消费，且最近90天内总消费天数小于3天,0:默认值',
  `pr`          int       COMMENT 'RFM90_R,1:最近90天有消费，且最近一次消费日期在最近30天内（含30）,2:最近90天有消费，且最近一次消费日期不在最近30天内（不含30）,0:默认值',
  `pn`          int       COMMENT 'RFM90_N,1:最近90天未消费，但90天之前消费过,2:用户激活时间小于等于7天，且用户从未消费过,3:用户激活时间大于7天小于等于30天，且用户从未消费过,4:用户激活时间大于30天，且用户从未消费过,0:默认值',
  `pimp`        int       COMMENT 'RFM90_重要价值'
 )COMMENT '用户推送rfm标签' PARTITIONED BY ( `pt` string) stored as parquetfile;