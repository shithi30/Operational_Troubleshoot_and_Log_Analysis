/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1PRpEOx6y_u93Hf5qyIG8A1C0X1HC2JJ_frqd3rTaxrg/edit#gid=668700995
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

with 
	err_tbl as
	(select 
		concat(event_name, ' | ', trim(regexp_replace(message, '[^[:alpha:]\s]', '', 'g'))) error, 
		app_version::int, 
		count(*) errors, 
		count(distinct user_id) users_effected
	from tallykhata.eventapp_event_temp 
	where 
		level='ERROR'
		and app_version is not null
		and user_id!=''
		and app_version::int in(116, 117, 118) 
	group by 1, 2
	) 
	
select *, errors/total_errors errors_pct_in_version
from 
	err_tbl tbl1 
	
	inner join 
	
	(select app_version, sum(errors) total_errors 
	from err_tbl 
	group by 1
	) tbl2 using(app_version)
order by 1; 

select 
	to_char(created_at, 'HH24') date_hour,
	count(case when date(created_at)='14-Nov-22' then 1 else null end) save_errors_14_nov_22, 
	count(case when date(created_at)='15-Nov-22' then 1 else null end) save_errors_15_nov_22 
from systems_monitoring.event_table_temp
where 
	level='ERROR' 
	and event_name='device_to_server_sync_v4'
	and message='failed to save'
	and date(created_at) in(current_date, current_date-1)
	and to_char(created_at, 'HH')::int<13
	and app_version::int=118
group by 1
order by 1 asc; 