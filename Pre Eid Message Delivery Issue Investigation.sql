/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=2105231737
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=895244723
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=53603178
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): Use personal task list for pivoting. 
*/

-- version-03: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=53603178
do $$

declare 
	var_date date:='28-Mar-22'::date; 
begin  
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.rec_analysis
		where report_date=var_date; 
	
		insert into data_vajapora.rec_analysis
		select var_date report_date, tg, count(mobile_no) merchants_received_msg
		from 
			(select distinct mobile_no
			from tallykhata.tallykhata_sync_event_fact_final
			where 
				created_date=var_date
				and event_name in('inbox_message_received', 'in_app_message_received')
			) tbl1 
			
			left join 
				
			(select 
				mobile_no, 
				case 
					when tg in('3RAUCb','3RAU Set-A','3RAU Set-B','3RAU Set-C','3RAUTa','3RAUTa+Cb','3RAUTacs') then '3RAU'
					when tg in('LTUCb','LTUTa') then 'LTU'
					when tg in('NB0','NN1','NN2-6') then 'NN'
					when tg in('NT--') then 'NT'
					when tg in('PSU') then 'PSU'
					when tg in('PUCb','PU Set-A','PU Set-B','PU Set-C','PUTa','PUTa+Cb','PUTacs') then 'PU'
					when tg in('SPU') then 'SU'
					when tg in('ZCb','ZTa','ZTa+Cb') then 'Zombie'
					else null
				end tg
			from 
				(select mobile_no, max(tg) tg
				from cjm_segmentation.retained_users
				where report_date=var_date
				group by 1
				) tbl1
			) tbl2 using(mobile_no)
		group by 1, 2; 
				
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date='07-May-22'::date+1 then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.rec_analysis;

-- version-01, 02: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=2105231737
select 
	schedule_date, 
	
	sum(case when tag_name='LTUAll' then intended_receiver_count else 0 end) intended_receive_count_ltu, 
	sum(case when tag_name='LTUAll' then total_success else 0 end) firebase_success_ltu, 
	sum(case when tag_name='LTUAll' then merchants_received_message else 0 end) messages_received_by_users_db_ltu, 
	
	sum(case when tag_name='PUAll' then intended_receiver_count else 0 end) intended_receive_count_pu, 
	sum(case when tag_name='PUAll' then total_success else 0 end) firebase_success_pu, 
	sum(case when tag_name='PUAll' then merchants_received_message else 0 end) messages_received_by_users_db_pu, 
	
	sum(case when tag_name='SPU' then intended_receiver_count else 0 end) intended_receive_count_su, 
	sum(case when tag_name='SPU' then total_success else 0 end) firebase_success_su, 
	sum(case when tag_name='SPU' then merchants_received_message else 0 end) messages_received_by_users_db_su, 
	
	sum(case when tag_name='ZAll' then intended_receiver_count else 0 end) intended_receive_count_zombie, 
	sum(case when tag_name='ZAll' then total_success else 0 end) firebase_success_zombie, 
	sum(case when tag_name='ZAll' then merchants_received_message else 0 end) messages_received_by_users_db_zombie
from 
	data_vajapora.all_sch_stats tbl1
	
	left join 
	
	(select event_date schedule_date, bulk_notification_id, sum(merchants_commited_event) merchants_received_message
	from data_vajapora.message_received_opened_stats
	where event_commited='received'	
	group by 1, 2
	) tbl2 using(schedule_date, bulk_notification_id)
	
	left join 
	
	(select event_date schedule_date, bulk_notification_id, sum(merchants_commited_event) merchants_opened_message
	from data_vajapora.message_received_opened_stats
	where event_commited='opened'	
	group by 1, 2
	) tbl3 using(schedule_date, bulk_notification_id)
	
	left join 
	
	(select 
		report_date schedule_date, 
		bulk_notification_id, 
		count(distinct mobile_no) open_through_inbox_merchants, 
		count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_merchants
	from data_vajapora.mom_cjm_performance_detailed
	group by 1, 2
	) tbl4 using(schedule_date, bulk_notification_id)
group by 1
order by 1;
