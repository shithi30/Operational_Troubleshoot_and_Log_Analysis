/*
- Viz: 
	Baki: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=420289888
	Tagada: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1404271448
	Cust Add: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=59610286
	Data Backup: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=384196937
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: https://docs.google.com/spreadsheets/d/1L8_w77CjgXsfoYDRh7N3Nm5PWTiJrqJRbSSMfEEVKZY/edit#gid=2035283517
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	Campaigns: https://docs.google.com/spreadsheets/d/1LtrEpjUcYbBToRkR8FWC7EPnj6Xh7mek2LWiH_rV4II/edit#gid=937912363
	Criteria: https://docs.google.com/spreadsheets/d/1L8_w77CjgXsfoYDRh7N3Nm5PWTiJrqJRbSSMfEEVKZY/edit?pli=1#gid=240389719
*/

-- event base results: bakir txn
do $$

declare 
	var_date date:='18-Aug-22'::date; 
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.event_base_credit_rec_res 
		where report_date=var_date; 
	
		-- recorded credit txn in last 14 days
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as 
		select distinct mobile_no 
		from tallykhata.tallykhata_fact_info_final 
		where 
			created_datetime>=var_date-14 and created_datetime<var_date 
			and txn_type like 'CREDIT%'; 
		
		-- recorded credit txn on report date
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select mobile_no, max(txn_timestamp) max_txn_timestamp
		from tallykhata.tallykhata_fact_info_final 
		where 
			created_datetime=var_date 
			and txn_type like 'CREDIT%'
		group by 1;  
		
		-- saw desired message
		drop table if exists data_vajapora.help_c; 
		create table data_vajapora.help_c as
		select mobile_no, min(event_timestamp) first_opened_msg
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date=var_date 
			and event_name like '%_message_open'
			and (bulk_notification_id, notification_id) in
				(select 
					bulk_notification_id, 
					case 
						when message_type='POPUP_MESSAGE' then (select id from notification_popupmessage where push_message_id=message_id)
						else message_id 
					end notification_id 
				from data_vajapora.all_sch_stats
				where campaign_id in('RBT220817-31', 'RBT220817-31i')   
				)
		group by 1; 
			
		-- results	
		insert into data_vajapora.event_base_credit_rec_res
		select 
			var_date report_date, 
			count(case when tbl2.mobile_no is null then tbl1.mobile_no else null end) merchants_didnt_rec_credit_txn_last_14_days, 
			count(case when tbl3.mobile_no is not null then tbl1.mobile_no else null end) merchants_rec_credit_txn, 
			count(case when tbl2.mobile_no is null and tbl3.mobile_no is not null then tbl1.mobile_no else null end) merchants_rec_credit_txn_after_14_days, 
			count(case when 
				tbl2.mobile_no is null 
				and tbl3.mobile_no is not null 
				and max_txn_timestamp>first_opened_msg
			then tbl1.mobile_no else null end) merchants_rec_credit_txn_after_14_days_after_seeing_msg
		from 
			(-- retained base on report_date
			select mobile_no 
			from cjm_segmentation.retained_users 
			where report_date=var_date 
			) tbl1 
			
			left join 
			
			data_vajapora.help_a tbl2 using(mobile_no) 
			
			left join 
			
			data_vajapora.help_b tbl3 using(mobile_no)
			
			left join 
		
			data_vajapora.help_c tbl4 using(mobile_no);
		
		commit; 
		raise notice 'Data generated for date: %', var_date;
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.event_base_credit_rec_res;

-- campaigns of interest
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select *
from data_vajapora.all_sch_stats
where 
	campaign_id in(
	'RBT220817-31', 
	'RBT220817-31i'
	); 

drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select 
	id, mobile_no, event_date, event_timestamp, bulk_notification_id, 
	case 
		when event_name like 'in_app%' then (select push_message_id from notification_popupmessage where id=notification_id) 
		else notification_id 
	end notification_id
from tallykhata.tallykhata_sync_event_fact_final
where 
	event_date>=(select min(schedule_date) from data_vajapora.help_a) and event_date<current_date
	and bulk_notification_id in(select bulk_notification_id from data_vajapora.help_a) 
	and event_name in('inbox_message_open', 'in_app_message_open');  

-- txns of interest
drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
select auto_id, mobile_no, created_datetime, txn_timestamp
from tallykhata.tallykhata_fact_info_final 
where 
	txn_type like '%CREDIT%'
	and created_datetime>=(select min(schedule_date) from data_vajapora.help_a) and created_datetime<current_date;  

select 
	event_date, 
	count(distinct mobile_no) txn_and_msg_view, 
	count(distinct case when event_timestamp<txn_timestamp then mobile_no else null end) txn_after_msg_view
from 
	(select mobile_no, event_date, min(event_timestamp) event_timestamp 
	from data_vajapora.help_b
	group by 1, 2
	) tbl1 
	
	inner join 
	
	(select mobile_no, created_datetime event_date, txn_timestamp 
	from data_vajapora.help_c
	) tbl2 using(mobile_no, event_date)
group by 1 
order by 1; 
