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

-- event base results: tagada
do $$ 

declare 
	var_date date:=current_date-30; 
begin 
	raise notice 'New OP goes below:'; 

	loop
		delete from data_vajapora.event_base_tagada_msg_res 
		where report_date=var_date::text; 
	
		-- TG
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select tbl1.mobile_no
		from 
			(select distinct mobile_no 
			from cjm_segmentation.retained_users 
			where report_date=var_date
			) tbl1 
			
			left join 
			
			(select mobile_no, count(id) tagada_used
			from public.tagada_log 
			where 
				tagada_type in('TAGADA_BY_SMS', 'TAGADA_BY_FREE_SMS') 
				and date(create_date)<var_date
			group by 1
			) tbl2 using(mobile_no)
		where 
			tagada_used is null
			or tagada_used<4; 
		
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
				where campaign_id in('ST220822-31-01')   
				)
		group by 1; 
		
		-- results
		insert into data_vajapora.event_base_tagada_msg_res
		select 
			var_date::text report_date, 
			(select count(mobile_no) from data_vajapora.help_a) tg,
			count(tbl1.mobile_no) merchants_used_tagada, 
			count(case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) merchants_used_tagada_campaign, 
			count(case when 
				tbl2.mobile_no is not null 
				and first_opened_msg<max_tagada_timespamp
			then tbl1.mobile_no else null end) merchants_used_tagada_campaign_after_msg
		from 
			(select mobile_no, max(create_date::timestamp) max_tagada_timespamp 
			from public.tagada_log 
			where 
				tagada_type in('TAGADA_BY_SMS', 'TAGADA_BY_FREE_SMS') 
				and date(create_date)=var_date
			group by 1
			) tbl1 
			
			left join 
			
			data_vajapora.help_a tbl2 using(mobile_no) 
			
			left join 
			
			data_vajapora.help_c tbl3 using(mobile_no);

		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
	
end $$; 

select *
from data_vajapora.event_base_tagada_msg_res;

-- campaigns of interest
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select *
from data_vajapora.all_sch_stats
where campaign_id in('ST220822-31-01'); 

-- events of interest
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

-- activities of interest
drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
select id, mobile_no, date(create_date) tagada_date, create_date tagada_time
from public.tagada_log 
where 
	tagada_type in('TAGADA_BY_SMS', 'TAGADA_BY_FREE_SMS')
	and date(create_date)>=(select min(schedule_date) from data_vajapora.help_a) and date(create_date)<current_date;  

select 
	event_date, 
	count(distinct mobile_no) tagada_and_msg_view, 
	count(distinct case when event_timestamp<tagada_time then mobile_no else null end) tagada_after_msg_view
from 
	(select mobile_no, event_date, min(event_timestamp) event_timestamp 
	from data_vajapora.help_b
	group by 1, 2
	) tbl1 
	
	inner join 
	
	(select mobile_no, tagada_date::date event_date, tagada_time::timestamp
	from data_vajapora.help_c
	) tbl2 using(mobile_no, event_date)
group by 1 
order by 1; 