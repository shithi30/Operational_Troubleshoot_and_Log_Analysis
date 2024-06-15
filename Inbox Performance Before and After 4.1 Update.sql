/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=43983950
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): averaging eliminated
*/

do $$

declare 
	var_date date:='2022-02-18'::date; 
	var_rdm_date_before date; 
	var_rdm_date_after date; 
begin  
	raise notice 'New OP goes below:'; 

	loop
		delete from data_vajapora.inbox_perf_before_after_41_update
		where report_date=var_date;
	
		-- first day in 4.1
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select mobile_no, min(report_date) update_date
		from cjm_segmentation.retained_users 
		where app_version='4.1'
		group by 1
		having min(report_date)=var_date; 
	
		var_rdm_date_before:=(select rdm_date_before from (select var_date-generate_series(1, 7) rdm_date_before) tbl1 where extract(dow from rdm_date_before) not in(5, 6) and rdm_date_before<current_date order by random() limit 1);
		var_rdm_date_after:=(select rdm_date_after from (select var_date+generate_series(1, 7) rdm_date_after) tbl1 where extract(dow from rdm_date_after) not in(5, 6) and rdm_date_after<current_date order by random() limit 1); 
		
		-- before 4.1
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select users_rec_inbox_before_41, users_open_inbox_before_41, users_opened_through_inbox_before_41
		from 
			(select 
				event_date, 
				count(distinct case when event_name='inbox_message_received' then mobile_no else null end) users_rec_inbox_before_41, 
				count(distinct case when event_name='inbox_message_open' then mobile_no else null end) users_open_inbox_before_41
			from 
				(select event_date, mobile_no, event_name
				from tallykhata.tallykhata_sync_event_fact_final 
				where 
					event_date=var_rdm_date_before                                             
					and event_name in('inbox_message_received', 'inbox_message_open')
				) tbl1 
				
				inner join 
				
				data_vajapora.help_a using(mobile_no)
			group by 1
			) tbl1,
			
			(select report_date, count(distinct mobile_no) users_opened_through_inbox_before_41
			from 
				data_vajapora.mom_cjm_performance_detailed tbl1 
				inner join 
				data_vajapora.help_a tbl2 using(mobile_no)
			where report_date=var_rdm_date_before                                                  
			group by 1	
			) tbl2; 
		
		-- after 4.1
		drop table if exists data_vajapora.help_c; 
		create table data_vajapora.help_c as
		select users_rec_inbox_after_41, users_open_inbox_after_41, users_opened_through_inbox_after_41
		from 
			(select 
				event_date, 
				count(distinct case when event_name='inbox_message_received' then mobile_no else null end) users_rec_inbox_after_41, 
				count(distinct case when event_name='inbox_message_open' then mobile_no else null end) users_open_inbox_after_41
			from 
				(select event_date, mobile_no, event_name
				from tallykhata.tallykhata_sync_event_fact_final 
				where 
					event_date=var_rdm_date_after
					and event_name in('inbox_message_received', 'inbox_message_open')
				) tbl1 
				
				inner join 
				
				data_vajapora.help_a using(mobile_no)
			group by 1
			) tbl1,
			
			(select report_date, count(distinct mobile_no) users_opened_through_inbox_after_41
			from 
				data_vajapora.mom_cjm_performance_detailed tbl1 
				inner join 
				data_vajapora.help_a tbl2 using(mobile_no)
			where report_date=var_rdm_date_after                                                  
			group by 1	
			) tbl2; 
		
		insert into data_vajapora.inbox_perf_before_after_41_update
		select var_date report_date, * 
		from 
			(select count(*) users_updated
			from data_vajapora.help_a
			) tbl1,
			data_vajapora.help_b, 
			data_vajapora.help_c; 
			
		commit; 
		raise notice 'Data generated for: %, %, %', var_date, var_rdm_date_before, var_rdm_date_after; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.inbox_perf_before_after_41_update; 
