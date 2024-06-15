/*
- Viz: https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=976899117
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
In correlation with Mahmud's analysis: https://docs.google.com/spreadsheets/d/1t-xPrZpP_ih9ke-bw1kTTDk0ok9CGNt0BjKNxqIx7bw/edit#gid=1228198900
*/

do $$ 

declare 
	var_date date:='2021-12-11'::date; 
begin 
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.zombie_msg_behavior 
		where report_date=var_date; 
	
		-- TG: zombies
		drop table if exists data_vajapora.help_c; 
		create table data_vajapora.help_c as
		select *
		from 
			(select mobile_no
			from cjm_segmentation.retained_users 
			where 
				report_date=var_date
				and tg ilike 'z%'
			) tbl1 
			
			inner join 
			
			(select mobile_number mobile_no
			from public.register_usermobile 
			where date(created_at)!=var_date
			) tbl2 using(mobile_no); 
		-- analyse data_vajapora.help_c; 
		
		-- sequenced events of zombies, filtered for necessary events
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select *
		from 
			(select id, mobile_no, event_timestamp, event_name, row_number() over(partition by mobile_no order by event_timestamp asc) seq
			from tallykhata.tallykhata_sync_event_fact_final
			where created_date=var_date
			) tbl1 
			
			inner join 
			
			data_vajapora.help_c tbl2 using(mobile_no)
		where event_name in('app_opened', 'inbox_message_open'); 
		-- analyse data_vajapora.help_a; 
		
		-- all push-open cases, with first opens of the day identified
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select tbl1.mobile_no, tbl3.id
		from 
			data_vajapora.help_a tbl1
			inner join 
			data_vajapora.help_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
			left join 
			(select mobile_no, min(id) id 
			from data_vajapora.help_a 
			where event_name='app_opened'
			group by 1
			) tbl3 on(tbl2.id=tbl3.id)
		where 
			tbl1.event_name='inbox_message_open'
			and tbl2.event_name='app_opened'; 
		-- analyse data_vajapora.help_b; 
		
		-- necessary statistics
		insert into data_vajapora.zombie_msg_behavior
		select 
			var_date report_date,
			(select count(distinct mobile_no) from data_vajapora.help_c) tg_zombie, 
			message_received_zombies, 
			(select count(distinct mobile_no) from data_vajapora.help_a where event_name='app_opened') app_opened_zombies,
			(select count(distinct mobile_no) from data_vajapora.help_a where event_name='inbox_message_open') inbox_opened_zombies,
			open_through_inbox_zombies, 
			first_open_through_inbox_zombies
		from 
			(select
				count(distinct mobile_no) open_through_inbox_zombies, 
				count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_zombies
			from data_vajapora.help_b
			) tbl1,
			
			(select count(distinct mobile_no) message_received_zombies
			from 
				(select mobile_no
				from tallykhata.tallykhata_sync_event_fact_final
				where 
					created_date=var_date
					and event_name='inbox_message_received'
				) tbl1 
				
				inner join 
			
				data_vajapora.help_c tbl2 using(mobile_no)
			) tbl2; 
		commit; 
	
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date='2021-12-13'::date then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.zombie_msg_behavior
where report_date>='2021-11-10'
order by 1; 
