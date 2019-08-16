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