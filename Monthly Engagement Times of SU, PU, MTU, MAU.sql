/*
- Viz: https://docs.google.com/spreadsheets/d/10oNv0UKadqbBXtMLiBk4C1BKU1wgnzyx3dZnPVTSMWY/edit#gid=1173344843
- Data: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit?pli=1#gid=1814790787
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	Sequence by Mahmud: 
	-- test.mau_last_3_months
	-- test.mtu_last_3_months
	-- test.su_last_3_months
	-- test.last_3_months_time_spent_summary
	-- avg. time spent for SU, PU, MTU, MAU
	
	-- August er MAU, MTU, SU
	-- eder test.august_time_spent_summary
	-- SU, MTU, MAU er avg. time spent
	
	-- test.august_time_spent_daily_summary_su
	-- test.august_time_spent_daily_summary_mtu
	-- test.august_time_spent_daily_summary_mau
	
	-- test.august_time_spent_summary theke jader 0 mins, oder version distribution 
*/

/* by Mahmud */

/*
Date: August 29, 2022
What is the avg time spent by MTU, SU and MAU?
*/

select * from test.userwise_time_spent ;

create table test.test_user_time as 
select * from test.userwise_time_spent 
where mobile_no ='01782188765'
and report_date >='2022-07-01' and report_date<'2022-08-01'
;

select * from test.test_user_time;

select 
	sum(cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min) as total_time_spent,
	count(1) as total_days_opened_app,
	sum(cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min)/count(1) as avg_daily_time_spent,
	avg(cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min) as avg_daily_time_spent_v2
from test.test_user_time;



-------------
--select TGs

create table test.mau_last_3_months as 
select 
	date_trunc('month', event_date)::date as months,
	mobile_no
from tallykhata.tallykhata_user_date_sequence_final
where event_date>='2022-05-01' and event_date<'2022-08-01'
and mobile_no !='0'
group by 1,2
;

select * from test.mau_last_3_months;

select months,count(1) from test.mau_last_3_months group by 1;

create table test.mtu_last_3_months as 
select 
	date_trunc('month', created_datetime)::date as months,
	mobile_no
from tallykhata.tallykhata_transacting_user_date_sequence_final
where created_datetime >='2022-05-01' and created_datetime<'2022-08-01'
and mobile_no !='0'
group by 1,2
;

select * from test.mtu_last_3_months;

select months,count(1) from test.mtu_last_3_months group by 1;


-- Only taking SU who became SU by transacting in that particular month (date: 30 and 31) (Excluding sticky)


drop table if exists test.su_last_3_months;
create table test.su_last_3_months as 
select 
	date_trunc('month', report_date)::date as months,
	mobile_no
from tallykhata.tk_spu_aspu_data
where 1=1
and pu_type='SPU' -- and pu_subtype is null
and report_date='2022-05-30' 

union all 

select 
	date_trunc('month', report_date)::date as months,
	mobile_no
from tallykhata.tk_spu_aspu_data
where 1=1
and pu_type='SPU' --and pu_subtype is null
and report_date='2022-06-30'

union all 

select 
	date_trunc('month', report_date)::date as months,
	mobile_no
from tallykhata.tk_spu_aspu_data
where 1=1
and pu_type='SPU' --and pu_subtype is null
and report_date='2022-07-30'
;

select * from test.su_last_3_months;

select months,count(1) from test.su_last_3_months group by 1;

------------------------------------





-- will take last 3 months data (May, Jun, July)

drop table if exists test.last_3_months_time_spent_summary;
create table test.last_3_months_time_spent_summary as 
select 
	date_trunc('month', report_date)::date as months,
	mobile_no,
	sum(cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min) as total_time_spent_per_month,
	count(case when cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min>0 then 1 end) as total_app_open_days
from test.userwise_time_spent 
where report_date >='2022-05-01' and report_date<'2022-08-01'
group by 1,2;
-- 2 min


select * from test.last_3_months_time_spent_summary;

----------------------
-- investigating
drop table if exists test.test_user_time;
create table test.test_user_time as 
select * from test.userwise_time_spent 
where mobile_no ='01710166142'
and report_date >='2022-05-01' and report_date<'2022-06-01'
;

select 
	*, 
	cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min,
	case when cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min>0 then 1 else 0 end as count_
from test.test_user_time;

select 
	*
from tallykhata.tallykhata_sync_event_fact_final
where event_date>='2022-05-01' and event_date<'2022-06-01'
and mobile_no ='01710166142'
;

-----------------------------------

--Analysis

-- Avg Time spent for SU
select 
	months,
	avg(avg_per_day_time_spent) as avg_per_day_time_spent,
	count(1) as total_su
from 
(
select 
	tbl2.*,
	case when total_app_open_days>0 then total_time_spent_per_month/total_app_open_days else 0 end as avg_per_day_time_spent
from test.su_last_3_months tbl1
inner join test.last_3_months_time_spent_summary tbl2 on tbl1.months=tbl2.months and tbl1.mobile_no=tbl2.mobile_no
) tbl3
group by 1
;


----------------------------
--investigating
select 
	*
from tallykhata.tk_spu_aspu_data
where 1=1
and pu_type='SPU' 
--and pu_subtype is null
and report_date>='2022-05-30' and report_date<'2022-06-01'
and mobile_no in ('01303590536')
;

select count(distinct create_date::date) from public.journal where mobile_no ='01303590536' and create_date ::date>='2022-05-01' and create_date<'2022-06-01'

select 
	*
from tallykhata.tallykhata_sync_event_fact_final
where event_date>='2022-05-01' and event_date<'2022-06-01'
and mobile_no ='01303590536'
;

-- User taken backup at the end of the month. But the function refreshes timespent for last 15 days.


select * from test.userwise_time_spent where report_date>='2022-05-01' and report_date<'2022-06-01' and mobile_no ='01303590536';
-------------------------

-- Avg Time spent for PU (Excluding SU)
select 
	months,
	avg(avg_per_day_time_spent) as avg_per_day_time_spent,
	count(1) as total_pu_excluding_su
from 
(
select 
	tbl2.*,
	case when total_app_open_days>0 then total_time_spent_per_month/total_app_open_days else 0 end as avg_per_day_time_spent
from test.pu_last_3_months_20220829 tbl1
inner join test.last_3_months_time_spent_summary tbl2 on tbl1.months=tbl2.months and tbl1.mobile_no=tbl2.mobile_no
where 1=1
--and not exists (select * from test.su_last_3_months tbl3 where tbl1.mobile_no=tbl3.mobile_no)
) tbl3
group by 1
;


-- Avg Time spent for MTU
select 
	months,
	avg(avg_per_day_time_spent) as avg_per_day_time_spent,
	count(1) as total_mtu
from 
(
select 
	tbl2.*,
	case when total_app_open_days>0 then total_time_spent_per_month/total_app_open_days else 0 end as avg_per_day_time_spent
from test.mtu_last_3_months tbl1
inner join test.last_3_months_time_spent_summary tbl2 on tbl1.months=tbl2.months and tbl1.mobile_no=tbl2.mobile_no
) tbl3
group by 1
;


-- Avg Time spent for MAU
select 
	months,
	avg(avg_per_day_time_spent) as avg_per_day_time_spent,
	count(1) as total_mau
from 
(
select 
	tbl2.*,
	case when total_app_open_days>0 then total_time_spent_per_month/total_app_open_days else 0 end as avg_per_day_time_spent
from test.mau_last_3_months tbl1
inner join test.last_3_months_time_spent_summary tbl2 on tbl1.months=tbl2.months and tbl1.mobile_no=tbl2.mobile_no
) tbl3
group by 1
;

------------------------------------
--Analysis for August
create table test.mau_august as 
select 
	date_trunc('month', event_date)::date as months,
	mobile_no
from tallykhata.tallykhata_user_date_sequence_final
where event_date>='2022-08-01' and event_date<'2022-09-01'
and mobile_no !='0'
group by 1,2
;
select * from test.mau_august;--740,786

create table test.mtu_august as 
select 
	date_trunc('month', created_datetime)::date as months,
	mobile_no
from tallykhata.tallykhata_transacting_user_date_sequence_final
where created_datetime >='2022-08-01' and created_datetime<'2022-09-01'
and mobile_no !='0'
group by 1,2
;
select * from test.mtu_august;--442,062

drop table if exists test.su_august;
create table test.su_august as 
select 
	date_trunc('month', report_date)::date as months,
	mobile_no
from tallykhata.tk_spu_aspu_data
where 1=1
and pu_type='SPU' --and pu_subtype is null
and report_date='2022-08-28'
;

select * from test.su_august;--83,129


drop table if exists test.august_time_spent_summary;
create table test.august_time_spent_summary as 
select 
	date_trunc('month', report_date)::date as months,
	mobile_no,
	sum(cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min) as total_time_spent_per_month,
	count(case when cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min>0 then 1 end) as total_app_open_days
from test.userwise_time_spent 
where report_date >='2022-08-01' and report_date<='2022-08-28'
group by 1,2;
-- 2 min

select * from test.august_time_spent_summary;
-----------
--Analysis

-- Avg Time spent for SU
select 
	months,
	avg(avg_per_day_time_spent) as avg_per_day_time_spent,
	count(1) as total_su
from 
(
select 
	tbl2.*,
	case when total_app_open_days>0 then total_time_spent_per_month/total_app_open_days else 0 end as avg_per_day_time_spent
from test.su_august tbl1
inner join test.august_time_spent_summary tbl2 on tbl1.months=tbl2.months and tbl1.mobile_no=tbl2.mobile_no
) tbl3
group by 1
;



----------------------------
--investigating

select count(distinct create_date::date) from public.journal where mobile_no ='01918829962' and create_date ::date>='2022-08-01' and create_date::date<current_date;

select 
	*
from tallykhata.tallykhata_sync_event_fact_final
where event_date>='2022-08-01' and event_date<current_date
and mobile_no ='01918829962'
;


select *,to_timestamp(event_start_time/1000) as event_timestamp from public.sync_appevent where tallykhata_user_id =2940989 and to_timestamp(event_start_time/1000)>='2022-08-01' and to_timestamp(event_start_time/1000)<current_date ;

select * from test.userwise_time_spent where report_date>='2022-05-01' and report_date<'2022-06-01' and mobile_no ='01303590536';

select 
	user_id ,
	event_name ,
	message 
from tallykhata.eventapp_event_temp where user_id ='01918829962' and created_at>= '2022-08-01' and created_at <current_date
and event_name ='sync_app_event'
;
-- No successful response  for event='sync_app_event' the user

-------------------------

-- Avg Time spent for MTU
select 
	months,
	avg(avg_per_day_time_spent) as avg_per_day_time_spent,
	count(1) as total_su
from 
(
select 
	tbl2.*,
	case when total_app_open_days>0 then total_time_spent_per_month/total_app_open_days else 0 end as avg_per_day_time_spent
from test.mtu_august tbl1
inner join test.august_time_spent_summary tbl2 on tbl1.months=tbl2.months and tbl1.mobile_no=tbl2.mobile_no
) tbl3
group by 1
;

-- Avg Time spent for MAU
select 
	months,
	avg(avg_per_day_time_spent) as avg_per_day_time_spent,
	count(1) as total_su
from 
(
select 
	tbl2.*,
	case when total_app_open_days>0 then total_time_spent_per_month/total_app_open_days else 0 end as avg_per_day_time_spent
from test.mau_august tbl1
inner join test.august_time_spent_summary tbl2 on tbl1.months=tbl2.months and tbl1.mobile_no=tbl2.mobile_no
) tbl3
group by 1
;


--------------------------
--Need to generate daywise avg for each segment
drop table if exists test.august_time_spent_daily_summary_su;
create table test.august_time_spent_daily_summary_su as 
select 
	report_date,
	sum(cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min) as total_time_spent,
	count(case when cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min>0 then 1 end) as total_user,
	sum(cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min)/count(case when cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min>0 then 1 end) as avg_time_spent
from test.userwise_time_spent tbl1
inner join test.su_august tbl2 on tbl1.mobile_no =tbl2.mobile_no
where report_date >='2022-08-01'  and report_date<='2022-08-28'
group by 1;

select * from  test.august_time_spent_daily_summary_su;

drop table if exists test.august_time_spent_daily_summary_mtu;
create table test.august_time_spent_daily_summary_mtu as 
select 
	report_date,
	sum(cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min) as total_time_spent,
	count(case when cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min>0 then 1 end) as total_user
from test.userwise_time_spent tbl1
inner join test.mtu_august tbl2 on tbl1.mobile_no =tbl2.mobile_no
where report_date >='2022-08-01' and report_date<='2022-08-28'
group by 1;

select * from  test.august_time_spent_daily_summary_mtu;

drop table if exists test.august_time_spent_daily_summary_mau;
create table test.august_time_spent_daily_summary_mau as 
select 
	report_date,
	sum(cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min) as total_time_spent,
	count(case when cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min>0 then 1 end) as total_user
from test.userwise_time_spent tbl1
inner join test.mau_august tbl2 on tbl1.mobile_no =tbl2.mobile_no
where report_date >='2022-08-01' and report_date<='2022-08-28'
group by 1;

select * from  test.august_time_spent_daily_summary_mau;

---------------------
--version wise distribution for SU whose app usage is 0 min

select * from test.su_august;
select * from test.august_time_spent_summary;

select 
	app_version,
	count(mobile_no) as total_su
from 
(
select 
	tbl1.mobile_no
from test.su_august tbl1
left join test.august_time_spent_summary tbl2 on tbl1.months=tbl2.months and tbl1.mobile_no=tbl2.mobile_no
where tbl2.mobile_no is null
) tbl3
inner join tallykhata.retained_users_daily tbl4 on tbl3.mobile_no=tbl4.mobile_number
group by 1
;


--checking few 4.1 users
01303254415 --blank
01310557399--blank
01318531646--blank
01401859579--blank
01404864612--blank
01581811615--blank
01601730273--blank
01601772672--blank

select *
from tallykhata.tallykhata_sync_event_fact_final
where 1=1
and event_date >='2022-08-01'
and created_date >='2022-08-01'
and mobile_no ='01601772672'
;

/* by Shithi */

-- daily stats
select 
	report_date, 
	tbl1.avg_time_spent su_avg_daily_mins_spent, 
	tbl2.total_time_spent*1.00/tbl2.total_user mtu_avg_daily_mins_spent,
	tbl3.total_time_spent*1.00/tbl3.total_user mau_avg_daily_mins_spent
from 
	test.august_time_spent_daily_summary_su tbl1 
	inner join 
	test.august_time_spent_daily_summary_mtu tbl2 using(report_date)
	inner join 
	test.august_time_spent_daily_summary_mau tbl3 using(report_date)
order by 1; 

-- monthly stats
select * 
from 
	(-- Avg Time spent for SU
	select 
		months,
		avg(avg_per_day_time_spent) as su_avg_daily_mins_spent/*,
		count(1) as total_su*/
	from 
	(
	select 
		tbl2.*,
		case when total_app_open_days>0 then total_time_spent_per_month/total_app_open_days else 0 end as avg_per_day_time_spent
	from test.su_last_3_months tbl1
	inner join test.last_3_months_time_spent_summary tbl2 on tbl1.months=tbl2.months and tbl1.mobile_no=tbl2.mobile_no
	) tbl3
	group by 1
	) tbl1 
	
	inner join 
	
	(-- Avg Time spent for MTU
	select 
		months,
		avg(avg_per_day_time_spent) as mtu_avg_daily_mins_spent/*,
		count(1) as total_mtu*/
	from 
	(
	select 
		tbl2.*,
		case when total_app_open_days>0 then total_time_spent_per_month/total_app_open_days else 0 end as avg_per_day_time_spent
	from test.mtu_last_3_months tbl1
	inner join test.last_3_months_time_spent_summary tbl2 on tbl1.months=tbl2.months and tbl1.mobile_no=tbl2.mobile_no
	) tbl3
	group by 1
	) tbl2 using(months)
	
	inner join 
	
	(-- Avg Time spent for MAU
	select 
		months,
		avg(avg_per_day_time_spent) as mau_avg_daily_mins_spent/*,
		count(1) as total_mau*/
	from 
	(
	select 
		tbl2.*,
		case when total_app_open_days>0 then total_time_spent_per_month/total_app_open_days else 0 end as avg_per_day_time_spent
	from test.mau_last_3_months tbl1
	inner join test.last_3_months_time_spent_summary tbl2 on tbl1.months=tbl2.months and tbl1.mobile_no=tbl2.mobile_no
	) tbl3
	group by 1
	) tbl3 using(months)
order by 1; 
