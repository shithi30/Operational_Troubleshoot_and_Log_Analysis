/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=866144668
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

do $$

declare 
	var_date date:='2022-02-16'::date; 
begin  
	raise notice 'New OP goes below:'; 

	loop
		delete from data_vajapora.msg_perf_before_after_41_update
		where report_date=var_date;
	
		-- first day in 4.1
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select mobile_no, min(report_date) update_date
		from cjm_segmentation.retained_users 
		where app_version='4.1'
		group by 1
		having min(report_date)=var_date; 
		
		-- before 4.1
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select 
			ceil(avg(users_rec_inbox)) users_rec_inbox_before_41, 
			ceil(avg(users_open_inbox)) users_open_inbox_before_41, 
			ceil(avg(users_rec_inapp)) users_rec_inapp_before_41, 
			ceil(avg(users_open_inapp)) users_open_inapp_before_41
		from 
			(select 
				event_date, 
				count(distinct case when event_name='inbox_message_received' then mobile_no else null end) users_rec_inbox, 
				count(distinct case when event_name='inbox_message_open' then mobile_no else null end) users_open_inbox, 
				count(distinct case when event_name='in_app_message_received' then mobile_no else null end) users_rec_inapp, 
				count(distinct case when event_name='in_app_message_open' then mobile_no else null end) users_open_inapp
			from 
				(select event_date, mobile_no, id, event_name
				from tallykhata.tallykhata_sync_event_fact_final 
				where 
					event_date>=var_date-7 and event_date<var_date
					and event_name in('inbox_message_received', 'inbox_message_open', 'in_app_message_received', 'in_app_message_open')
				) tbl1 
				
				inner join 
				
				data_vajapora.help_a using(mobile_no)
			group by 1
			) tbl1; 
		
		-- after 4.1
		drop table if exists data_vajapora.help_c; 
		create table data_vajapora.help_c as
		select 
			ceil(avg(users_rec_inbox)) users_rec_inbox_after_41, 
			ceil(avg(users_open_inbox)) users_open_inbox_after_41, 
			ceil(avg(users_rec_inapp)) users_rec_inapp_after_41, 
			ceil(avg(users_open_inapp)) users_open_inapp_after_41
		from 
			(select 
				event_date, 
				count(distinct case when event_name='inbox_message_received' then mobile_no else null end) users_rec_inbox, 
				count(distinct case when event_name='inbox_message_open' then mobile_no else null end) users_open_inbox, 
				count(distinct case when event_name='in_app_message_received' then mobile_no else null end) users_rec_inapp, 
				count(distinct case when event_name='in_app_message_open' then mobile_no else null end) users_open_inapp
			from 
				(select event_date, mobile_no, id, event_name
				from tallykhata.tallykhata_sync_event_fact_final 
				where 
					event_date>=var_date and event_date<(case when var_date+7>current_date-1 then current_date else var_date+7 end)
					and event_name in('inbox_message_received', 'inbox_message_open', 'in_app_message_received', 'in_app_message_open')
				) tbl1 
				
				inner join 
				
				data_vajapora.help_a using(mobile_no)
			group by 1
			) tbl1; 
		
		insert into data_vajapora.msg_perf_before_after_41_update		
		select var_date report_date, * 
		from 
			(select count(*) users_updated
			from data_vajapora.help_a
			) tbl1,
			data_vajapora.help_b, 
			data_vajapora.help_c; 
			
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.msg_perf_before_after_41_update; 
