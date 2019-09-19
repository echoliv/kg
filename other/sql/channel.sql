频道推荐：

set hive.auto.convert.join=false;
set hive.exec.compress.output=false;
set hive.exec.reducers.bytes.per.reducer=8000000;
set tez.queue.name=${ql};
set hive.execution.engine=tez;
set tez.job.name=${job_name};
create table if not exists analyse.ll_all_recommend_song_channel
(
     songid   string,
     channel  string
)partitioned by (dt string)
row format delimited fields terminated by '|' lines terminated by '\n' stored as textfile;
insert overwrite table analyse.ll_all_recommend_song_channel partition (dt='${yyyy_mm_dd}')

select songid,channel
from
(
select songid,channel,row_number() over(partition by songid order by cossim desc) rank
from
(
  select songid2 songid ,channel,sum(cossim) cossim
  from
  (
    select t2.songid2,t1.channel,t1.songid,t2.cossim
    from
    (
      select channel,songid
      from analyse.ll_all_channel_song
      where dt='${yyyy_mm_dd}'
    )t1 join
    (
      select songid1,songid2 ,cossim,rank
      from analyse.ljm_behavior_simsong_filtered_all_langfix
      where dt='${d}' and cossim>=0.4 and rank <=200
    )t2 on t1.songid =t2.songid1
    group by t2.songid2,t1.channel,t1.songid,t2.cossim
  )t21 group by songid2,channel
)t31
)t41  where rank <=10

