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

-- event base results: data backup
do $$

declare 
	var_date date:=current_date-50; 
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.event_base_data_bkp_res 
		where report_date=var_date; 
	
		-- TG
		/* drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select tallykhata_user_id, id, to_timestamp(event_start_time/1000) event_datetime
		from public.sync_appevent
		where event_name ='manual_data_backup'; */ 
					
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select mobile_no, count(id) data_backups
		from 
			(select distinct mobile_no
			from cjm_segmentation.retained_users 
			where 
				report_date=var_date
				and tg not like 'Z%'
			) tbl1 
			
			left join 
			
			(select tallykhata_user_id, mobile_number mobile_no
			from public.register_usermobile 
			) tbl2 using(mobile_no)
			
			left join 
			
			(select tallykhata_user_id, id 
			from data_vajapora.help_b
			where date(event_datetime)<var_date
			) tbl3 using(tallykhata_user_id) 
		group by 1 
		having count(id)<3; 
		
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
				where campaign_id in('DB220607-13-01', 'DB220607-13-02', 'DB220607-13-03', 'DB220607-13-04', 'AC220608-13-05', 'AC220608-13-06', 'AC220608-13-07', 'AC220608-13-08', 'AC220608-13-05', 'AC220608-13-06', 'AC220608-13-07', 'AC220608-13-08', 'DB220607-13-01', 'DB220607-13-02', 'DB220607-13-03', 'DB220607-13-04', 'DB220701-01', 'DB220701-02', 'DB220701-03', 'DB220701-04', 'DB220702-01', 'DB220702-02', 'DB220702-03', 'DB220702-04', 'DB220703-01', 'DB220703-02', 'DB220703-03', 'DB220703-04', 'DB220801-01', 'DB220801-02', 'DB220801-03', 'DB220801-04', 'DB220802-01', 'DB220802-02', 'DB220802-03', 'DB220802-04', 'DB220803-01', 'DB220803-02', 'DB220803-03', 'DB220803-04', 'DB220705-31-01', 'DB220705-31-02', 'DB220705-31-03', 'DB220705-31-04', 'DB220705-31-05', 'DB220705-31-06', 'DB220705-31-07', 'DB220705-31-08', 'DB220705-31-09', 'DB220705-31-10', 'DB220805-31-01', 'DB220806-31-02', 'DB220807-31-03', 'DB220804-31-04', 'DB220805-31-05', 'DB220805-31-06', 'DB220813-31-07', 'DB220813-31-08', 'DB220804-31-09', 'DB220804-31-10', 'DB220901-01', 'DB220901-02', 'DB220901-03', 'DB220901-04', 'DB220902-01', 'DB220902-02', 'DB220902-03', 'DB220902-04', 'DB220903-01', 'DB220903-02', 'DB220903-03', 'DB220903-04', 'DB220904-01', 'DB220904-02', 'DB220904-03', 'DB220905-01', 'DB220905-02', 'DB220905-03', 'DB220906-01', 'DB220906-02', 'DB220907-01', 'DB220907-02', 'DB220908-30-01', 'DB220908-30-02', 'DB220908-30-03', 'DB220908-30-04', 'DB220908-30-05', 'DB220908-30-06', 'DB220908-30-07', 'DB220908-30-08', 'DB220908-30-09', 'DB220908-30-10')   
				)
		group by 1; 
		
		-- results
		insert into data_vajapora.event_base_data_bkp_res
		select 
			var_date report_date, 
			(select count(mobile_no) from data_vajapora.help_a) tg, 
			count(tbl1.mobile_no) merchants_bkp, 
			count(case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) merchants_bkp_campaign, 
			count(case when 
				tbl2.mobile_no is not null 
				and first_opened_msg<max_backup_timespamp
			then tbl1.mobile_no else null end) merchants_bkp_campaign_after_msg
		from 
			(select mobile_no, max(event_timestamp) max_backup_timespamp 
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_name ='manual_data_backup'
				and event_date=var_date
			group by 1
			) tbl1 
			
			left join 
			
			data_vajapora.help_a tbl2 using(mobile_no) 
			
			left join 
			
			data_vajapora.help_c using(mobile_no);
		
		commit; 
		raise notice 'Data generated for date: %', var_date;
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.event_base_data_bkp_res; 

-- event base results: add customer
do $$

declare 
	var_date date:=current_date-30; 
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.event_base_cust_rec_res 
		where report_date=var_date; 
	
		-- TG
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select mobile_no, count(contact) added_custs 
		from 
			(select distinct mobile_no
			from cjm_segmentation.retained_users 
			where 
				report_date=var_date
				and tg not like 'Z%'
			) tbl1 
			
			left join 
			
			(select mobile_no, contact 
			from public.account
			where 
				"type"=2 
				and is_active is true
				and date(create_date)<var_date
			) tbl2 using(mobile_no) 
		group by 1 
		having count(contact)<5; 
	
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
				where campaign_id in('AC220908-30-01', 'AC220908-30-01', 'AC220607-13-01', 'AC220607-13-02', 'AC220607-13-04', 'AC220607-13-05', 'AC220608-13-01', 'AC220608-13-02', 'AC220608-13-03', 'AC220608-13-04', 'AC220701-01', 'AC220701-02', 'AC220701-03', 'AC220701-04', 'AC220702-01', 'AC220702-02', 'AC220702-03', 'AC220702-04', 'AC220703-01', 'AC220703-02', 'AC220703-03', 'AC220703-04', 'AC220801-01', 'AC220801-02', 'AC220801-03', 'AC220801-04', 'AC220802-01', 'AC220802-02', 'AC220802-03', 'AC220802-04', 'AC220803-01', 'AC220803-02', 'AC220803-03', 'AC220803-04', 'AC220608-13-01', 'AC220608-13-02', 'AC220608-13-03', 'AC220608-13-04', 'AC220607-13-01', 'AC220607-13-02', 'AC220607-13-04', 'AC220607-13-05', 'AC220705-31-01', 'AC220705-31-02', 'AC220705-31-03', 'AC220705-31-04', 'AC220705-31-05', 'AC220705-31-06', 'AC220705-31-07', 'AC220705-31-08', 'AC220705-31-09', 'AC220705-31-10', 'AC220805-31-01', 'AC220806-31-02', 'AC220804-31-03', 'AC220813-31-04', 'AC220805-31-05', 'AC220805-31-06', 'AC220813-31-07', 'AC220813-31-08', 'AC220404-31-09', 'AC220804-31-10', 'AC220901-01', 'AC220901-02', 'AC220901-03', 'AC220901-04', 'AC220902-01', 'AC220902-02', 'AC220902-03', 'AC220902-04', 'AC220903-01', 'AC220903-02', 'AC220903-03', 'AC220903-04', 'AC220904-01', 'AC220904-02', 'AC220904-03', 'AC220905-01', 'AC220905-02', 'AC220905-04', 'AC220906-01', 'AC220906-02', 'AC220907-01', 'AC220907-02', 'AC220907-03', 'DB220907-03', 'AC220909-30-01', 'AC220910-30-01', 'AC220911-30-01', 'AC220908-30-02', 'AC220908-30-03', 'AC220908-30-04', 'AC220908-30-05', 'AC220908-30-06')   
				)
		group by 1; 
		
		-- results
		insert into data_vajapora.event_base_cust_rec_res
		select 
			var_date report_date, 
			(select count(mobile_no) from data_vajapora.help_a) tg, 
			count(tbl1.mobile_no) merchants_added_customers, 
			count(case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) merchants_added_customers_campaign, 
			count(case when 
				tbl2.mobile_no is not null 
				and first_opened_msg<max_add_cust_timespamp
			then tbl1.mobile_no else null end) merchants_added_customers_campaign_after_msg
		from 
			(select mobile_no, max(create_date::timestamp) max_add_cust_timespamp 
			from public.account
			where 
				"type"=2 
				and is_active is true
				and date(create_date)=var_date
			group by 1
			) tbl1 
			
			left join 
			
			data_vajapora.help_a tbl2 using(mobile_no) 
			
			left join 
			
			data_vajapora.help_c tbl3 using(mobile_no);
		
		commit; 
		raise notice 'Data generated for date: %', var_date;
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.event_base_cust_rec_res; 
