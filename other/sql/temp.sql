#!/bin/bash
##********************************************************************#
##
## 日期支持运算，通过以下方式：
## ${DATA_DATE offset field formatter}，
## DATE_DATE：*固定值，为当前作业的业务时间
## offet：*必填，当前的日期偏移量，根据field判定偏移字段，取值为数值可正可负
## field：*必填，偏移的字段，取值可以为：day，month，year，minute，hour，week，second
## formatter：选填，计算偏移后的日期格式，如：yyyy-MM-dd HH:mm:ss
## 如：${DATA_DATE -1 day 'yyyy-MM-dd HH:mm'}
##********************************************************************#
############################## 引入公共函数库和日期变量 ################################
source $BIPROG_ROOT/bin/shell/common.sh
vDay=${DATA_DATE}
yyyy_mm_dd=`date -d "$vDay" +%Y-%m-%d`


############################### 主程序 ############################

sql="
set hive.auto.convert.join=false;
set hive.exec.compress.output=false;
set tez.queue.name=${q};
set hive.execution.engine=tez;
set tez.job.name=special_collect;
create table if not exists analyse.ll_eve_special_train_lable
(
     userid       string,
     specialid    string,
     lable        string
)partitioned by (dt string)
row format delimited fields terminated by '|' lines terminated by '\n' stored as textfile;

insert overwrite table analyse.ll_eve_special_train_lable partition (dt='${yyyy_mm_dd}')
select userid, specialid ,lable
from
(
    select userid ,specialid ,1 as lable
    from
    analyse.ll_eve_special_collect_specialid
    where dt ='${yyyy_mm_dd}'
    union all
    select userid,specialid,1 as lable
    from
    analyse.ll_eve_special_collect_songid
    where dt = '${yyyy_mm_dd}'
    group by userid,specialid
	union all
	select userid,specialid,lable
	from
	analyse.ll_eve_special_play_duration_lable
	where dt='${yyyy_mm_dd}' and lable =1
)t1 group by userid, specialid ,lable
;
"
EXE_HIVE "$sql"

sql="
set hive.auto.convert.join=false;
set hive.exec.compress.output=false;
set tez.queue.name=${q};
set hive.execution.engine=tez;
set tez.job.name=special_sample;
insert overwrite table analyse.ll_eve_special_train_lable partition (dt='${yyyy_mm_dd}')

select userid, specialid ,lable,0 as tag
from analyse.ll_eve_special_train_lable
where dt='${yyyy_mm_dd}' and lable =1

union all
select t1.userid, t2.specialid,t2.lable
from
(
select userid
from analyse.ll_eve_special_train_lable
where dt='${yyyy_mm_dd}'
group by userid
)t1
join
(
select userid,specialid,0 as lable
from
analyse.ll_eve_special_display_noplay
where dt='${yyyy_mm_dd}'

union all
select userid,specialid,lable
from
analyse.ll_eve_special_play_duration_lable
where dt='${yyyy_mm_dd}' and lable=0

union all

select userid, specialid 0 as lable
from
analyse.ll_eve_special_display_noopen
where dt='${yyyy_mm_dd}'
)t2 on t1.userid =t2.userid
group by
t1.userid, t2.specialid,t2.lable
;
"

sql="
set hive.auto.convert.join=false;
set hive.exec.compress.output=false;
set tez.queue.name=${q};
set hive.execution.engine=tez;
set hive.cli.print.header=false;
select count(*) from analyse.ll_eve_special_train_lable where dt='${yyyy_mm_dd}' and lable=1;
"
a=hive -e "$sql"
echo $a

sql="
set hive.auto.convert.join=false;
set hive.exec.compress.output=false;
set tez.queue.name=${q};
set hive.execution.engine=tez;
set tez.job.name=special_sample;
insert overwrite table analyse.ll_eve_special_train_lable partition (dt='${yyyy_mm_dd}')
select userid, specialid ,lable
from analyse.ll_eve_special_train_lable
where dt='${yyyy_mm_dd}' and lable =1

union all

select * from
"