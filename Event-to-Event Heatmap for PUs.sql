/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/1cjSEKk4_o9wQN1gcggJWBD6yhnZZCm5i70rEWjXfYaY/edit#gid=67992154
	- https://docs.google.com/spreadsheets/d/1cjSEKk4_o9wQN1gcggJWBD6yhnZZCm5i70rEWjXfYaY/edit#gid=1125202766
	- https://docs.google.com/spreadsheets/d/1cjSEKk4_o9wQN1gcggJWBD6yhnZZCm5i70rEWjXfYaY/edit#gid=124713394
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): 
	- PU er heatmap
	- 1 week e kothay gharaghuri korlo
	- last 1 month e 25 days er beshi PU te
	- 10k first week e koi ghurlo
	- PU er ager 1 week
	- last 1 week after regular
	- PU na, emon 3 rau er jonno same
	- PU na, 3RAU na, eder jiboner first week er ghuraghuri status pawar age, last 1 week
*/

/* heatmaps for PUs */

-- PUs of the last 30 days, who never dropped the status
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select *
from 
	(select mobile_no, count(distinct report_date) pu_days, max(report_date) max_pu_date
	from tallykhata.tk_power_users_10
	where report_date>=current_date-30 and report_date<current_date
	group by 1 
	) tbl1 
	
	inner join 
	
	(select mobile_no, min(report_date) min_pu_date
	from tallykhata.tk_power_users_10
	group by 1
	) tbl2 using(mobile_no)
where pu_days=30; 

-- among those PUs, recent regs with various 7-day intervals
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select *, reg_date+6 reg_plus_6_days, max_pu_date-6 max_pu_minus_6_days, min_pu_date-6 min_pu_minus_6_days
from 
	data_vajapora.help_a tbl1 
	inner join 
	(select mobile_number mobile_no, date(created_at) reg_date
	from public.register_usermobile 
	) tbl2 using(mobile_no)
where reg_date>=current_date-75 and reg_date<current_date-30
order by random() 
limit 10000; 

-- sequenced events of those PUs within 7 days of latest PU status
drop table if exists data_vajapora.help_c;
create table data_vajapora.help_c as
select tbl1.mobile_no, event_name, event_timestamp, row_number() over(partition by tbl1.mobile_no order by event_timestamp asc) event_seq
from 
	data_vajapora.help_b tbl1
	
	inner join 
	
	(select mobile_no, event_name, event_date, event_timestamp
	from tallykhata.tallykhata_sync_event_fact_final
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and event_date>=max_pu_minus_6_days and event_date<=max_pu_date); 

-- sequenced events of those PUs within 7 days of first PU status
drop table if exists data_vajapora.help_d;
create table data_vajapora.help_d as
select tbl1.mobile_no, event_name, event_timestamp, row_number() over(partition by tbl1.mobile_no order by event_timestamp asc) event_seq
from 
	data_vajapora.help_b tbl1
	
	inner join 
	
	(select mobile_no, event_name, event_date, event_timestamp
	from tallykhata.tallykhata_sync_event_fact_final
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and event_date>=min_pu_minus_6_days and event_date<=min_pu_date); 

-- sequenced events of those PUs within 7 days of reg
drop table if exists data_vajapora.help_e;
create table data_vajapora.help_e as
select tbl1.mobile_no, event_name, event_timestamp, row_number() over(partition by tbl1.mobile_no order by event_timestamp asc) event_seq
from 
	data_vajapora.help_b tbl1
	
	inner join 
	
	(select mobile_no, event_name, event_date, event_timestamp
	from tallykhata.tallykhata_sync_event_fact_final
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and event_date>=reg_date and event_date<=reg_plus_6_days);

-- event-to-event movements of regular PUs: within 7 days of latest PU status
select 
	tbl1.event_name from_event, 
	tbl2.event_name to_event, 
	count(distinct tbl1.mobile_no) merchants_moved,
	count(distinct tbl1.mobile_no)*1.00/(select count(mobile_no) from data_vajapora.help_b) merchants_moved_pct
from 
	data_vajapora.help_c tbl1
	inner join 
	data_vajapora.help_c tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.event_seq=tbl2.event_seq-1)
group by 1, 2; 

-- event-to-event movements of regular PUs: within 7 days of first PU status
select 
	tbl1.event_name from_event, 
	tbl2.event_name to_event, 
	count(distinct tbl1.mobile_no) merchants_moved,
	count(distinct tbl1.mobile_no)*1.00/(select count(mobile_no) from data_vajapora.help_b) merchants_moved_pct
from 
	data_vajapora.help_d tbl1
	inner join 
	data_vajapora.help_d tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.event_seq=tbl2.event_seq-1)
group by 1, 2; 

-- event-to-event movements of regular PUs: within 7 days of reg
select 
	tbl1.event_name from_event, 
	tbl2.event_name to_event, 
	count(distinct tbl1.mobile_no) merchants_moved,
	count(distinct tbl1.mobile_no)*1.00/(select count(mobile_no) from data_vajapora.help_b) merchants_moved_pct
from 
	data_vajapora.help_e tbl1
	inner join 
	data_vajapora.help_e tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.event_seq=tbl2.event_seq-1)
group by 1, 2; 
