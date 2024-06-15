/*
- Viz: 
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 

I need stats of GAID. What percent of users who are currently in 3.0.1 and has synced data is missing the GAID?
GAID --> public.register_tallykhatauser TBL (advertise_id Column)

*/

drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select mobile_no 
from tallykhata.tallykhata_sync_event_fact_final 
where event_date>='2021-06-01' 
union 
select mobile_no
from public.journal 
where date(create_date)>='2021-06-01'
union 
select mobile_no
from public.account
where date(create_date)>='2021-06-01'; 

drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select *
from 
	(select mobile_no, max(id) id
	from public.register_tallykhatauser
	group by 1
	) tbl1 
	
	inner join 
	
	(select id, mobile_no, advertise_id 
	from public.register_tallykhatauser
	) tbl2 using(mobile_no, id); 

select 
	count(mobile_no) in_301_and_synced_merchants,
	count(case when advertise_id is null then mobile_no else null end) missing_gaid,
	count(case when advertise_id is null then mobile_no else null end)*1.00/count(distinct mobile_no) missing_gaid_pct
from 
	(select mobile_no, latest_version
	from tallykhata.tk_user_app_version
	where latest_version='3.0.1'
	) tbl1
	
	inner join 
	
	data_vajapora.help_a tbl2 using(mobile_no)
	
	left join
	
	data_vajapora.help_b tbl3 using(mobile_no); 
