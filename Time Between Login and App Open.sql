/*
- Viz: https://docs.google.com/spreadsheets/d/1g2Hm6qmYJjB870MluhkS1y2-kDYfIGNLTOiCfb-cPdY/edit#gid=826348311
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
*/

drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select *, row_number() over(partition by user_id order by created_at asc) seq 
from 
	(-- login requests
	select user_id, 'login' event_name, created_at, app_version
	from tallykhata.eventapp_event_temp 
	where 
		((event_name like '%login%' and message like '%request received%') or (event_name='/api/auth/init' and message like 'response generated for%') or (event_name='auth-verify-sign-in'))
		and date(created_at) in(current_date-1, current_date)
	union all
	-- app opens
	select mobile_no user_id, 'app_opened' event_name, event_timestamp created_at, '' app_version
	from tallykhata.tallykhata_sync_event_fact_final 
	where 
		event_date in(current_date-1, current_date)
		and event_name in('app_opened', 'app_launched')
	) tbl1; 
		
-- to pivot
select 
	app_version, 
	case 
		when mins_delay in(0, 1) then 'within 1 min' 
		when mins_delay=2 then 'within 2 mins' 
		when mins_delay=3 then 'within 3 mins' 
		when mins_delay=4 then 'within 4 mins' 
		when mins_delay=5 then 'within 5 mins' 
		when mins_delay>5 then 'within more than 5 mins' 
	end mins_delay_cat,
	count(*) login_events 
from 
	(select tbl1.user_id, tbl1.app_version, date_part('minute', tbl2.created_at-tbl1.created_at) mins_delay
	from 
		data_vajapora.help_b tbl1 
		inner join 
		data_vajapora.help_b tbl2 on(tbl1.user_id=tbl2.user_id and tbl1.seq=tbl2.seq-1)
	where 
		tbl1.event_name='login' 
		and tbl2.event_name='app_opened' 
	) tbl1 
group by 1, 2 
order by 1, 2; 