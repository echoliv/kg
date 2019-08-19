select from t2.type ,count(*)
(select   specialid from analyse.ll_ugc_superuser_special where dt='2019-08-12'
)t1 join
(select specialid,
case when publish_time <'2015-00-00' then '<2015'
case when publish_time between '2015-00-00' and '2017-00-00' then '2015-2016'
case when publish_time between '2017-00-00' and '2018-00-00' then '2017-2018'
else '2019' end as type
else '>30000' end as type   from common.k_special_part where dt='2019-08-12'
group by
pecialid,
case when publish_time <'2015-00-00' then '<2015'
case when publish_time between '2015-00-00' and '2017-00-00' then '2015-2016'
case when publish_time between '2017-00-00' and '2018-00-00' then '2017-2018'
else '2019' end
)t2 on t1.specialid =t2.specialid group by t2.type;

select count(*)
from
(
select songid1,songid2
from analyse.ll_special_similar_filter_all_rank_1
where dt='2019-08-12' and rank<300
)t1
left join
(select songid1,songid2 from  analyse.ljm_behavior_simsong_filtered_all_pay3
where dt='2019-08-12' and rank<300 and cossim>0.4
)t2 on (t1.songid1=t2.songid1 and t1.songid2=t2.songid2)
where t2.songid1 is null and t2.songid2 is null

select t1.* from
(
select * from analyse.ll_special_similar_filter_language_publishtime_top300_cos_all
and rank <50
)t1
left join
(
select songid1 from analyse.ljm_behavior_simsong_filtered_all_langfix
where dt='2019-08-12' group by songid1
)t2 on t1.songid1 =t2.songid2 where t2.songid1 is null
limit 300;


select count(distinct(specialid)),count(distinct(userid)),sum(play_count)
from
dsl.restruct_dwm_list_all_play_d
where dt='2019-08-18' and cast(userid as int)>0 and fo like '%搜索/%/歌单%'
and cast(special_id as int)>0 and pt='android'


select count(distinct(t2.global_collection_id)),count(distinct(userid)) usercount,sum(playcount)
from
(
select special_id specialid,userid,sum(play_count) playcount
from
dsl.restruct_dwm_list_all_play_d
where dt='2019-08-18' and cast(userid as int)>0 and fo like '%搜索/%/歌单%'
and cast(special_id as int)>0 and pt='android'
group by special_id,userid
)t1
join
(
select specialid ,global_collection_id
from
common.k_special
where
is_publish =1
group by specialid ,global_collection_id
union all
select global_collection_id specialid,global_collection_id from
common.k_special
where is_publish=1
group by global_collection_id ,global_collection_id
)t2 on t1.specialid =t2.specialid

#########################################################
select t3.ranktype,count(distinct(t3.songid)),sum(play_pv)
from
(
select hash ,sum(play_pv) play_pv
from dsl.dww_individuation_play_list where dt in ('2019-08-17','2019-08-18')
and (concat(substring(i, -1), '-', cast(substring(i, -3, 2) as int) % 10))='4-1'
group by hash
)t1 join
(
select songid,hash
from
analyse.lhy_st_k_song_file_part where dt='2019-08-17'
)t2 on upper(t1.hash)=upper(t2.hash)
join
(
select songid,
case when rank between 1 and 1000 then '(1,1000)'
when rank between  1001 and 5000 then '(1000,5000)'
when rank between  5001 and 50000 then '(10000,50000)'
when rank between  50001 and 100000 then '(50000,100000)'
when rank between  100001 and 500000 then '(100000,500000)'
when rank between  500001 and 1000000 then '(500000,1000000)'
else '>100w' end as ranktype
from mini_dsl.zhongzhao_fm_songid_usercnt_rank
where dt='2019-08-16'
)t3 on t2.songid =t3.songid
group by t3.ranktype