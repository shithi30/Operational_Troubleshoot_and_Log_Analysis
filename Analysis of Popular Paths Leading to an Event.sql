/*
- Viz: 
- Data: 
- Function: 
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): A specified day's reg. day churns are analysed
*/

-- 24 hours' activity after reg
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select tbl1.mobile_no, event_name, event_timestamp, row_number() over(partition by tbl1.mobile_no order by event_timestamp) event_seq
from 
	(select mobile_number mobile_no, created_at reg_datetime, (created_at+interval '24 hour') reg_datetime_plus_24_hr 
	from public.register_usermobile 
	where date(created_at)='2021-04-25'
	) tbl1
	
	inner join 
	
	(select mobile_no, event_name, event_timestamp
	from tallykhata.tallykhata_sync_event_fact_final 
	where event_date in('2021-04-25', '2021-04-25'::date+1) -- to engulf 24 hours after reg
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl2.event_timestamp>=tbl1.reg_datetime and tbl2.event_timestamp<=tbl1.reg_datetime_plus_24_hr)

	inner join 
		
	(-- identifying reg day churns
	select mobile_no 
	from 
		(select mobile_no, max(created_datetime) last_act_date
		from tallykhata.tallykhata_fact_info_final 
		group by 1 
		having current_date-max(created_datetime)>=14
		) tbl1 
		
		inner join 
		
		(select mobile_number mobile_no, date(created_at) reg_date 
		from public.register_usermobile 
		where date(created_at)='2021-04-25'
		) tbl2 using(mobile_no)
	where last_act_date=reg_date
	) tbl3 on(tbl1.mobile_no=tbl3.mobile_no); 

-- popular paths
select path_to_help, count(distinct mobile_no), count(mobile_no) merchants_traversed
from 
	(select *, concat(split_part(events_sequenced, 'help', 1), 'help') path_to_help
	from 
		(select mobile_no, string_agg(event_name::varchar, ', ' order by event_seq asc) events_sequenced
		from data_vajapora.help_b
		group by 1 
		) tbl1
	where events_sequenced like '%help%'
	) tbl1 
group by 1 
order by 2 desc; 
