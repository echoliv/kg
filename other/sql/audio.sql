select substring(userid,-1),count(distinct(userid)),sum(eff_play_count),sum(eff_play_duration)
from
dal.la_normal_mid_collect_detail
where dt='2019-08-07' and fo like '/电台/有声电台/推荐/%' and substring(tv,0,3)='930' group by substring(userid,-1);

select count(distinct(t1.userid)),count(*) from
(
select userid,albumid
from
dal.la_normal_mid_collect_detail
where dt='2019-08-07' and fo like '/电台/有声电台/推荐/%' and  substring(userid,-1) =8
group by userid,albumid
)t1
join
(
select userid,albumid
from analyse.ll_audiobook_album_recommend_result
where dt='2019-08-05'
and rank <=6 and substring(userid,-1) =8
)t2
on t1.userid =t2.userid and t1.albumid=t2.albumid
;