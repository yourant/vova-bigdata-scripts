[9531]招商提成商品数据
https://zt.gitvv.com/index.php?m=task&f=view&taskID=34754

https://docs.google.com/spreadsheets/d/1HieTqPpfcn9eh6JhCVKyqIHe_zBOHsqCj1DGPRmeutg/edit#gid=1138901608

任务描述
https://docs.google.com/spreadsheets/d/1HieTqPpfcn9eh6JhCVKyqIHe_zBOHsqCj1DGPRmeutg/edit#gid=1138901608

需求内容
dashboard名：招商提成商品数据
表一：提成标准
计算周期    品类  月销额阈值
表二：商品达标明细
计算周期    品类  有效商品组   有效商品ID  招商名称
表三：提成汇总
计算周期    品类  招商名称    提成商品数   提成单价    提成金额

维度说明
筛选项 枚举值
计算周期    商品销额计算周期
一级品类

表名  字段名 字段业务口径（定义、公式或逻辑）
提成标准         计算周期      商品销额计算周期，按自然月展示，按天更新
                一级品类
                月销额阈值    以商品组为单位,将商品组该月GMV倒序排列计算月销售额阈值

商品达标明细     计算周期      商品销额计算周期，按自然月展示，按天更新
                一级品类
                有效商品ID    "在月销阈值之上的商品组里的商品ID
                              --非brand商品
                              --ID对应的店铺为近3个月的新激活店铺
                              --店铺近3个月与其他店铺无关联"
                原始商品组号   在月销阈值之上的商品组
                招商名称       商品所在店铺的招商BD

提成汇总         计算周期       商品销额计算周期，按自然月展示，按天更新
                一级品类
                招商名称
                提成商品数     表2的有效商品ID数的汇总
                提成商金额     一个有效商品ID提成200

月销额阈值:
每个品类算一个方差，商品组该月gmv按降序排列，并以此计算相邻商品组月gmv差值，阈值定为相邻商品组月gmv差值小于方差的上一个商品组的月gmv

提成商品数,提成商金额:
计算提成商品数时，需要按月排除之前提成过的有效商品所在的商品组（需记录每个月被提成过的商品所在的原始商品组号）
同一商品组仅提成一次,goods_id 提成也只能提一次,只要加入测试集的商品,在任何月份跑出都计提提成

文档：
https://confluence.gitvv.com/pages/viewpage.action?pageId=21276399

-- 提成标准
create table dwb.dwb_vova_royalty_norm
(
    first_cat_id         bigint        COMMENT '一级品类ID',
    first_cat_name       string        COMMENT '一级品类名称',
    month_sale_threshold decimal(14,4) COMMENT '月销额阈值',
    rank_threshold       decimal(14,4) COMMENT '商品序数阈值'

) COMMENT '招商提成报表-提成标准'
PARTITIONED BY ( pt string) STORED AS PARQUETFILE
;

-- t2: 商品达标明细
create table dwb.dwb_vova_goods_reach_norm_detail
(
    first_cat_id          bigint        COMMENT '一级品类ID',
    first_cat_name        string        COMMENT '一级品类名称',
    goods_id              bigint        COMMENT '有效商品ID',
    group_id              bigint        COMMENT '原始商品组号',
    spsor_name            string        COMMENT '招商名称'
) COMMENT '招商提成报表-商品达标明细'
PARTITIONED BY(pt string) STORED AS PARQUETFILE
;

drop table dwb.dwb_vova_goods_group_inc;
create table dwb.dwb_vova_goods_group_inc
(
    first_cat_id          bigint        COMMENT '一级品类ID',
    group_id              bigint        COMMENT '原始商品组号',
    goods_id              bigint        COMMENT '原始商品ID'
) COMMENT '招商提成报表-商品达标明细-已计算过的商品组(每月增量)'
PARTITIONED BY(pt string) STORED AS PARQUETFILE
;

-- t3: 提成汇总
create table dwb.dwb_vova_commission
(
    first_cat_id          bigint        COMMENT '一级品类ID',
    first_cat_name        string        COMMENT '一级品类名称',
    spsor_name            string        COMMENT '招商名称',
    goods_cnt             bigint        COMMENT '提成商品数',
    commission            bigint        COMMENT '提成金额'
) COMMENT '招商提成报表-提成汇总'
PARTITIONED BY(pt string) STORED AS PARQUETFILE
;

























