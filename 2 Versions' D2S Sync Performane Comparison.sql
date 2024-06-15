/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1162580321
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): Useful after new version rollout. 
*/

do $$

declare

begin 
	-- deleting last 24 hours' data
	delete from data_vajapora.version_shift_d2s_behaviour
	where date_hr in(select distinct to_char(created_at, 'YYYY-MM-DD hour: HH') from tallykhata.eventapp_event_temp); 
	
	-- appending last 24 hours' data afresh
	insert into data_vajapora.version_shift_d2s_behaviour
	select 
		to_char(created_at, 'YYYY-MM-DD hour: HH24') date_hr, 
		count(distinct case when app_version::int=97 then user_id else null end) d2s_request_merchants_version_97,
		count(distinct case when app_version::int=96 then user_id else null end) d2s_request_merchants_version_96,
		count(distinct case when app_version::int=97 and message like '%response%' then user_id else null end) d2s_success_merchants_version_97,
		count(distinct case when app_version::int=96 and message like '%response%' then user_id else null end) d2s_success_merchants_version_96,
		count(distinct case when app_version::int=97 and message like '%response%' then user_id else null end)*1.00/
		count(distinct case when app_version::int=97 then user_id else null end)
		d2s_success_merchants_version_97_pct,
		count(distinct case when app_version::int=96 and message like '%response%' then user_id else null end)*1.00/
		count(distinct case when app_version::int=96 then user_id else null end)
		d2s_success_merchants_version_96_pct
	from tallykhata.eventapp_event_temp
	where event_name like '%device_to_server%' 
	group by 1
	order by 1; 
end $$; 

select *
from data_vajapora.version_shift_d2s_behaviour; 
