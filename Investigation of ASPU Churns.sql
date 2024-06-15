/*
- Viz: https://docs.google.com/spreadsheets/d/1g2Hm6qmYJjB870MluhkS1y2-kDYfIGNLTOiCfb-cPdY/edit?pli=1#gid=1848277602
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any):
	Initial Findings: 
	- There is a rising trend in ASPUs churning permanently from the group. 
	- Since ASPUs depend on TRT, TRV, Tally TRV and engagement time, last 60 days' churns show:
	  - A falling trend in TRT
	  - A falling trend in engagement time
	  These two factors may be causing the churns.
	
	- 25 ASPUs churned within 7 days of device change. 
	- 28 ASPUs churned within 7 days of upgrading to 5.x. 
	- 11 ASPUs' data did not sync after churn

*/

-- ASPU earn/churn distribution
select 
	tbl1.report_date, 
	count(tbl1.mobile_no) aspus, 
	count(case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) aspus_cont, 
	count(case when tbl2.mobile_no is null then tbl1.mobile_no else null end) aspus_earned, 
	count(case when tbl2.mobile_no is null and tbl1.report_date=min_report_date then tbl1.mobile_no else null end) aspus_earned_new, 
	count(case when tbl2.mobile_no is null and tbl1.report_date!=min_report_date then tbl1.mobile_no else null end) aspus_earned_winback, 
	count(case when tbl4.mobile_no is null then tbl1.mobile_no else null end) aspus_churned, 
	count(case when tbl4.mobile_no is null and tbl1.report_date=max_report_date then tbl1.mobile_no else null end) aspus_churned_permanently
from 
	(select report_date, mobile_no 
	from tallykhata.tk_spu_aspu_data 
	where 
		pu_type='ASPU' 
		and report_date>=current_date-65 and report_date<current_date
	) tbl1 
	
	left join 
	
	(select report_date, mobile_no 
	from tallykhata.tk_spu_aspu_data 
	where 
		pu_type='ASPU' 
		and report_date>=current_date-65 and report_date<current_date
	) tbl2 on(tbl2.report_date=tbl1.report_date-1 and tbl1.mobile_no=tbl2.mobile_no)
	
	left join 
		
	(select report_date, mobile_no 
	from tallykhata.tk_spu_aspu_data 
	where 
		pu_type='ASPU' 
		and report_date>=current_date-65 and report_date<current_date
	) tbl4 on(tbl4.report_date=tbl1.report_date+1 and tbl1.mobile_no=tbl4.mobile_no)
	
	left join 
	
	(select mobile_no, min(report_date) min_report_date, max(report_date) max_report_date
	from tallykhata.tk_spu_aspu_data 
	where pu_type='ASPU' 
	group by 1
	) tbl3 on(tbl1.mobile_no=tbl3.mobile_no)
group by 1; 

-- permanent ASPU churn cases
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select tbl1.*
from 
	(select report_date, mobile_no 
	from tallykhata.tk_spu_aspu_data 
	where 
		pu_type='ASPU' 
		and report_date>=current_date-65 and report_date<current_date
	) tbl1 
	
	left join 
	
	(select report_date, mobile_no 
	from tallykhata.tk_spu_aspu_data 
	where 
		pu_type='ASPU' 
		and report_date>=current_date-65 and report_date<current_date
	) tbl2 on(tbl2.report_date=tbl1.report_date-1 and tbl1.mobile_no=tbl2.mobile_no)
	
	left join 
		
	(select report_date, mobile_no 
	from tallykhata.tk_spu_aspu_data 
	where 
		pu_type='ASPU' 
		and report_date>=current_date-65 and report_date<current_date
	) tbl4 on(tbl4.report_date=tbl1.report_date+1 and tbl1.mobile_no=tbl4.mobile_no)
	
	left join 
	
	(select mobile_no, min(report_date) min_report_date, max(report_date) max_report_date
	from tallykhata.tk_spu_aspu_data 
	where pu_type='ASPU' 
	group by 1
	) tbl3 on(tbl1.mobile_no=tbl3.mobile_no) 
where 
	tbl4.mobile_no is null 
	and tbl1.report_date=max_report_date
	and tbl1.report_date>=current_date-65 and tbl1.report_date<current_date; 

/*version-01*/

-- ASPU: All-txn super power user
-- 1. >=30 txn days in total
-- 2. 20 txn/week
-- 3. 20 min/week
-- 4. TRV >= 3* Tally TRV (last 30 days)

drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select 
	created_datetime, 
	count(1) txns, 
	sum(input_amount) trv, 
	sum(case when txn_type like '%CREDIT%' then input_amount else null end) tally_trv 
from 
	(select mobile_no, input_amount, created_datetime, txn_type 
	from tallykhata.tallykhata_fact_info_final 
	where created_datetime>=current_date-60 and created_datetime<current_date 
	) tbl1 
	
	inner join 
	
	(select mobile_no
	from data_vajapora.help_a 
	where report_date>='19-Aug-22' and report_date<'20-Oct-22'
	) tbl2 using(mobile_no) 
group by 1; 

select * 
from 
	data_vajapora.help_b tbl1 
	
	inner join 

	(select event_date created_datetime, sum(sec_with_tk)/60.00 sum_mins_spent 
	from 
		(select mobile_no, event_date, sec_with_tk
		from tallykhata.daily_times_spent_individual_data
		where event_date>=current_date-60 and event_date<current_date 
		) tbl1 
		
		inner join 
			
		(select mobile_no
		from data_vajapora.help_a 
		where report_date>='19-Aug-22' and report_date<'20-Oct-22'
		) tbl2 using(mobile_no) 
	group by 1
	) tbl2 using(created_datetime);

/*version-02*/

drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select *
from 
	(-- churns to investigate
	select report_date churn_date, mobile_no
	from data_vajapora.help_a 
	where report_date>='20-Oct-22'::date-7 and report_date<'20-Oct-22' -- 13-Oct to 19-Oct
	) tbl1 
	
	left join 
	
	(-- upgrade to 5.x
	select update_date, mobile_no
	from data_vajapora.version_info 
	) tbl2 using(mobile_no)
	
	left join 
	
	(-- device change
	select user_id mobile_no, min(date(created_at)) device_change_date
	from systems_monitoring.event_table_temp 
	where 
		event_name in(
		'inactive_device_activate',
		'/api/device/activate',
		'device_activate'
		) 
	group by 1
	) tbl3 using(mobile_no); 

-- churned after device change
select * 
from data_vajapora.help_b
where churn_date>=device_change_date; 

-- churned after update
select * 
from data_vajapora.help_b
where churn_date>=update_date; 

-- churned within 7 days of device change
select * 
from data_vajapora.help_b
where churn_date>=device_change_date and churn_date<=device_change_date+7; 

-- churned within 7 days of 5.x update
select * 
from data_vajapora.help_b
where churn_date>=update_date and churn_date<=update_date+7; 

-- sync issues

-- churned after device change
select * 
from 
	data_vajapora.help_b tbl1 
	
	left join 
	
	(select user_id mobile_no, max(created_at) last_sync_time 
	from systems_monitoring.event_table_temp 
	where 
		event_name like '%device_to_server%' 
		and message like '%response generated%'
	group by 1 
	) tbl2 using(mobile_no)
where 
	churn_date>=device_change_date
	and date(last_sync_time)<=churn_date 
	
union

-- churned after update
select * 
from 
	data_vajapora.help_b tbl1 
		
	left join 
	
	(select user_id mobile_no, max(created_at) last_sync_time 
	from systems_monitoring.event_table_temp 
	where 
		event_name like '%device_to_server%' 
		and message like '%response generated%'
	group by 1 
	) tbl2 using(mobile_no)
where 
	churn_date>=update_date 
	and date(last_sync_time)<=churn_date; 
