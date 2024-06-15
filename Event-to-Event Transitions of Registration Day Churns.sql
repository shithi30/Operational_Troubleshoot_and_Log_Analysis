/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1316066104
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 
*/

do $$ 

declare
	-- allow enough time to churn
	var_date date:=current_date-15; 
begin
	-- 24 hours' activity after reg
	drop table if exists data_vajapora.help_b;
	create table data_vajapora.help_b as
	select tbl1.mobile_no, event_name, event_timestamp, row_number() over(partition by tbl1.mobile_no order by event_timestamp) event_seq
	from 
		(select mobile_number mobile_no, created_at reg_datetime, (created_at+interval '24 hour') reg_datetime_plus_24_hr 
		from public.register_usermobile 
		where date(created_at)=var_date
		) tbl1
		
		inner join 
		
		(select mobile_no, event_name, event_timestamp
		from tallykhata.tallykhata_sync_event_fact_final 
		where event_date in(var_date, var_date+1) -- to engulf 24 hours after reg
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl2.event_timestamp>=tbl1.reg_datetime and tbl2.event_timestamp<=tbl1.reg_datetime_plus_24_hr); 
	
	-- merchants' movements to immediate next events
	drop table if exists data_vajapora.help_c;
	create table data_vajapora.help_c as
	select 
		from_event, 
		to_event, 
		count(distinct mobile_no) merchants_moved,
		count(distinct mobile_no)*1.00/(select count(distinct mobile_no) from data_vajapora.help_b) merchants_moved_pct,
		var_date reg_date
	from 
		(select tbl1.mobile_no, tbl1.event_name from_event, tbl2.event_name to_event
		from 
			data_vajapora.help_b tbl1 
			inner join 
			data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.event_seq=tbl2.event_seq-1)
		) tbl1
		
		inner join 
		
		(-- identifying reg day churns
		select mobile_no 
		from 
			(select mobile_no, max(created_datetime) last_act_date
			from data_vajapora.user_date_seq 
			group by 1 
			having current_date-max(created_datetime)>=14
			) tbl1 
			
			inner join 
			
			(select mobile_number mobile_no, date(created_at) reg_date 
			from public.register_usermobile 
			where date(created_at)=var_date
			) tbl2 using(mobile_no)
		where last_act_date=reg_date
		) tbl2 using(mobile_no)
	group by 1, 2; 
end $$; 

select *
from data_vajapora.help_c;

		