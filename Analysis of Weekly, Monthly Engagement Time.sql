/*
- Viz: 
	- Daily: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=297659295
	- Weekly: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1467676642
	- Monthly: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=95519366
- Data: 
- Function: tallykhata.fn_daily_times_spent_individual()
- Table:
	- tallykhata.daily_avg_engagement_time
	- tallykhata.weekly_avg_engagement_time
	- tallykhata.monthly_avg_engagement_time
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 
*/

drop table if exists tallykhata.temp_ev_tbl_help_3; 
create table tallykhata.temp_ev_tbl_help_3 as 
select *
from 
	(select mobile_no, event_date, sec_with_tk
	from tallykhata.daily_times_spent_individual
	) tbl1 
	
	left join 
	
	(select rau_date rau_10_date, mobile_no rau_10_mobile_no
	from tallykhata.tallykahta_regular_active_user_new
	where rau_category=10
	) tbl2 on(tbl1.mobile_no=tbl2.rau_10_mobile_no and tbl1.event_date=tbl2.rau_10_date)
	
	left join 
	
	(select rau_date rau_3_date, mobile_no rau_3_mobile_no
	from tallykhata.tallykhata_regular_active_user
	where rau_category=3
	) tbl3 on(tbl1.mobile_no=tbl3.rau_3_mobile_no and tbl1.event_date=tbl3.rau_3_date)
	
	left join 
	
	(select report_date fau_date, mobile fau_mobile_no
	from tallykhata.fau_for_dashboard
	where category in('fau', 'fau-1')
	) tbl4 on(tbl1.mobile_no=tbl4.fau_mobile_no and tbl1.event_date=tbl4.fau_date)
	
	left join 
	
	(select roaming_date, mobile_no roamer_mobile_no
	from tallykhata.roaming_users
	) tbl5 on(tbl1.mobile_no=tbl5.roamer_mobile_no and tbl1.event_date=tbl5.roaming_date)
	
	left join 
	
	(select distinct report_date pu_date, mobile_no pu_mobile_no
	from data_vajapora.tk_power_users_10
	) tbl6 on(tbl1.mobile_no=tbl6.pu_mobile_no and tbl1.event_date=tbl6.pu_date); 

drop table if exists tallykhata.daily_avg_engagement_time;
create table tallykhata.daily_avg_engagement_time as
select 
	event_date, 
	avg(case when roaming_date is not null then sec_with_tk else null end)/60.00 roaming_dau_avg_min_with_tk,
	avg(sec_with_tk)/60.00 dau_avg_min_with_tk,
	avg(case when rau_3_date is not null then sec_with_tk else null end)/60.00 rau_3_avg_min_with_tk,
	avg(case when rau_10_date is not null then sec_with_tk else null end)/60.00 rau_10_avg_min_with_tk,
	avg(case when fau_date is not null then sec_with_tk else null end)/60.00 fau_avg_min_with_tk,
	avg(case when pu_date is not null then sec_with_tk else null end)/60.00 pu_avg_min_with_tk
from tallykhata.temp_ev_tbl_help_3
group by 1 
order by 1 asc; 
-- raise notice 'Daily engagement times are calculated'; 

drop table if exists tallykhata.weekly_avg_engagement_time;
create table tallykhata.weekly_avg_engagement_time as
select 
	date_part('year', event_date) "year",
	date_part('week', event_date) "week",
	
	min(event_date) week_start, 
	max(event_date) week_end, 
	concat(min(event_date), ' to ', max(event_date)) week_start_to_end, 
	
	sum(case when roaming_date is not null then sec_with_tk else 0 end)/(count(distinct roamer_mobile_no)*60.00) roaming_dau_avg_min_with_tk,
	sum(sec_with_tk)/(count(distinct mobile_no)*60.00) dau_avg_min_with_tk,
	sum(case when rau_3_date is not null then sec_with_tk else 0 end)/(count(distinct rau_3_mobile_no)*60.00) rau_3_avg_min_with_tk,
	sum(case when rau_10_date is not null then sec_with_tk else 0 end)/(count(distinct rau_10_mobile_no)*60.00) rau_10_avg_min_with_tk,
	sum(case when fau_date is not null then sec_with_tk else 0 end)/(count(distinct fau_mobile_no)*60.00) fau_avg_min_with_tk,
	sum(case when pu_date is not null then sec_with_tk else 0 end)/(count(distinct pu_mobile_no)*60.00) pu_avg_min_with_tk
from tallykhata.temp_ev_tbl_help_3
group by 1, 2
order by 3 asc; 
-- raise notice 'Weekly engagement times are calculated'; 

drop table if exists tallykhata.monthly_avg_engagement_time;
create table tallykhata.monthly_avg_engagement_time as
select 
	date_part('year', event_date) "year",
	date_part('month', event_date) "month",
	
	left(min(event_date)::varchar, 7) year_month, 
	
	sum(case when roaming_date is not null then sec_with_tk else 0 end)/(count(distinct roamer_mobile_no)*60.00) roaming_dau_avg_min_with_tk,
	sum(sec_with_tk)/(count(distinct mobile_no)*60.00) dau_avg_min_with_tk,
	sum(case when rau_3_date is not null then sec_with_tk else 0 end)/(count(distinct rau_3_mobile_no)*60.00) rau_3_avg_min_with_tk,
	sum(case when rau_10_date is not null then sec_with_tk else 0 end)/(count(distinct rau_10_mobile_no)*60.00) rau_10_avg_min_with_tk,
	sum(case when fau_date is not null then sec_with_tk else 0 end)/(count(distinct fau_mobile_no)*60.00) fau_avg_min_with_tk,
	sum(case when pu_date is not null then sec_with_tk else 0 end)/(count(distinct pu_mobile_no)*60.00) pu_avg_min_with_tk
from tallykhata.temp_ev_tbl_help_3
group by 1, 2
order by 1, 2; 
-- raise notice 'Monthly engagement times are calculated'; 
