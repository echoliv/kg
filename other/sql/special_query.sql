select concat(t21.specialid,'|',t21.specialname,'|',t22.singername,'|',t22.songname)
from
(
select t2.specialid,t3.specialname,t3.creator,t3.total_play_count,t2.pre,t2.cou
from
(select specialid,pre,cou
from
(
select t02.specialid,cou2/cou pre,cou
from
(
select specialid,count(distinct(userid)) cou
from
analyse.ll_eve_special_display_noplay
where dt>='2019-07-01' and dt<='2019-07-20' group by specialid
having cou >100
)t01 join
(
select specialid,count(distinct(userid)) cou2
from dt>='2019-07-01' and dt<='2019-07-20'
where dt ='2019-08-01'
and (fo rlike '^/歌单(/全部分类)?/推荐' or fo rlike '首页/个性化推荐/歌单')
group by specialid
)t02 on t01.specialid=t02.specialid
) t1 order by pre limit 300
)t2
join
(
select specialid,specialname,total_play_count,creator
from common.k_special_part where dt='2019-08-05'
)t3 on t2.specialid =t3.specialid order by t2.pre limit 300
)t21 join
(
select specialid,singername,songname
from common.k_special_song_part
where dt='2019-08-06'
)t22 on  t21.specialid=t22.specialid
;


select t2.specialid,t3.specialname,t3.creator,t3.total_play_count,t2.pre,t2.cou
from
(select specialid,pre,cou
from
(
select t02.specialid,cou2/(cou+cou2) pre,cou
from
(
select t002.specialid,t001.cou
from
(
select split(specialid,':')[0],count(distinct(userid)) cou
from
analyse.ll_eve_special_display_noopen
where dt>='2019-07-31' group by split(specialid,':')[0]
having cou >100
)t001 join
(
select specialid,global_collection_id
from common.k_special
group by specialid,global_collection_id
union all
select specialid,specialid global_collection_id
from common.k_special
group by specialid,specialid
)t002 on t001.specialid =t002.global_collection_id
)t01 join
(
select specialid,count(distinct(userid)) cou2
from analyse.ll_eve_special_play_records
where dt >='2019-07-31'
and (fo rlike '^/歌单(/全部分类)?/推荐' or fo rlike '首页/个性化推荐/歌单')
group by specialid
)t02 on t01.specialid=t02.specialid
) t1 order by pre limit 100
)t2
join
(
select specialid,specialname,total_play_count,creator
from common.k_special_part where dt='2019-08-05'
)t3 on t2.specialid =t3.specialid order by t2.pre limit 100;


insert overwrite table analyse.ll_eve_special_play_duration partition (dt='2019-08-01')
select t1.userid,t2.global_collection_id,t1.play_duration,t1.cou,t1.rank
from
(
select userid,specialid,play_duration,cou,rank
from
analyse.ll_eve_special_play_duration
where dt ='2019-08-01' and specialid IS NOT NULL  AND specialid<>'NULL'
)t1
join
(
select specialid,global_collection_id
from common.k_special
group by specialid,global_collection_id
union all
select global_collection_id specialid,global_collection_id
from common.k_special
group by global_collection_id,global_collection_id
)t2 on t1.specialid=t2.specialid
group by t1.userid,t2.global_collection_id,t1.play_duration,t1.cou,t1.rank
;

