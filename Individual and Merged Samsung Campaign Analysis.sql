/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit?pli=1#gid=1330139715
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
	Import import data_vajapora.samsung_numbers from https://docs.google.com/spreadsheets/d/1kViyTjWtoVxdY6wcH6vzPhE8YV2C1RfEdTVVGUAz-wA/edit#gid=0
*/

-- import data_vajapora.samsung_numbers

-- campaigns of interest
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select 
	*, 
	case 
		when message_type='POPUP_MESSAGE' then (select id from notification_popupmessage where push_message_id=message_id)
		else message_id 
	end notification_id 
from data_vajapora.all_sch_stats
where campaign_id in('STD220909-01', 'STD220909-02', 'STD220908-01', 'STD220908-02'); 

-- events of interest
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select * 
from 
	(select 
		id, mobile_no, 
		event_date, event_timestamp, event_name,
		bulk_notification_id, notification_id
	from tallykhata.tallykhata_sync_event_fact_final
	where 
		event_date>=(select min(schedule_date) from data_vajapora.help_a) and event_date<current_date-1
		and (notification_id, bulk_notification_id) in(select notification_id, bulk_notification_id from data_vajapora.help_a)
	) tbl1
		
	inner join 
	
	(select notification_id, bulk_notification_id, max(campaign_id) campaign_id
	from data_vajapora.help_a
	group by 1, 2
	) tbl2 using(notification_id, bulk_notification_id); 

-- desired stats

-- seperate
select 
	campaign_id, message_id, 
	message_type, message, 
	message_received, message_opened, 
	link_tapped, link_tapped_ltu, link_tapped_pu, link_tapped_su, link_tapped_3rau,  
	link_tapped_and_ordered, link_tapped_and_ordered_ltu, link_tapped_and_ordered_pu, link_tapped_and_ordered_su, link_tapped_and_ordered_3rau, 
	opened_via_notification, first_opened_via_notification
from 
	(select 
		campaign_id, 
		max(message_id) message_id, 
		max(message_type) message_type, 
		max(message) message
	from data_vajapora.help_a 
	group by 1
	) tbl0 
	
	left join 

	(select 
		campaign_id, 
		
		count(distinct case when event_name like '%_message_received' then mobile_no else null end) message_received, 
		count(distinct case when event_name like '%_message_open' then mobile_no else null end) message_opened, 
		
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') then mobile_no else null end) link_tapped,
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in(select mobile_no from cjm_segmentation.retained_users where report_date=current_date-1 and tg in('LTUCb','LTUTa')) then mobile_no else null end) link_tapped_ltu, 
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in(select mobile_no from cjm_segmentation.retained_users where report_date=current_date-1 and tg in('PUCb','PU Set-A','PU Set-B','PU Set-C','PUTa','PUTa+Cb','PUTacs')) then mobile_no else null end) link_tapped_pu, 
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in(select mobile_no from cjm_segmentation.retained_users where report_date=current_date-1 and tg in('SPU')) then mobile_no else null end) link_tapped_su, 
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in(select mobile_no from cjm_segmentation.retained_users where report_date=current_date-1 and tg in('3RAUCb','3RAU Set-A','3RAU Set-B','3RAU Set-C','3RAUTa','3RAUTa+Cb','3RAUTacs')) then mobile_no else null end) link_tapped_3rau, 
		
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in (select right("ফোন"::text, 11) from data_vajapora.samsung_numbers) then mobile_no else null end) link_tapped_and_ordered,         
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in (select right("ফোন"::text, 11) from data_vajapora.samsung_numbers) and mobile_no in(select mobile_no from cjm_segmentation.retained_users where report_date=current_date-1 and tg in('LTUCb','LTUTa')) then mobile_no else null end) link_tapped_and_ordered_ltu, 
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in (select right("ফোন"::text, 11) from data_vajapora.samsung_numbers) and mobile_no in(select mobile_no from cjm_segmentation.retained_users where report_date=current_date-1 and tg in('PUCb','PU Set-A','PU Set-B','PU Set-C','PUTa','PUTa+Cb','PUTacs')) then mobile_no else null end) link_tapped_and_ordered_pu, 
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in (select right("ফোন"::text, 11) from data_vajapora.samsung_numbers) and mobile_no in(select mobile_no from cjm_segmentation.retained_users where report_date=current_date-1 and tg in('SPU')) then mobile_no else null end) link_tapped_and_ordered_su, 
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in (select right("ফোন"::text, 11) from data_vajapora.samsung_numbers) and mobile_no in(select mobile_no from cjm_segmentation.retained_users where report_date=current_date-1 and tg in('3RAUCb','3RAU Set-A','3RAU Set-B','3RAU Set-C','3RAUTa','3RAUTa+Cb','3RAUTacs')) then mobile_no else null end) link_tapped_and_ordered_3rau            
	from data_vajapora.help_b
	group by 1
	) tbl1 using(campaign_id)
	
	left join 
	
	(select 
		campaign_id, 
		count(distinct mobile_no) opened_via_notification, 
		count(distinct case when id is not null then mobile_no else null end) first_opened_via_notification
	from 
		tallykhata.mom_cjm_performance_detailed tbl1
		inner join 
		(select notification_id, bulk_notification_id, max(campaign_id) campaign_id
		from data_vajapora.help_a
		group by 1, 2
		) tbl2 using(notification_id, bulk_notification_id)
	where (notification_id, bulk_notification_id) in(select notification_id, bulk_notification_id from data_vajapora.help_a)
	group by 1
	) tbl2 using(campaign_id)
	
union all
	
-- merged
select 
	'merged' campaign_id, null message_id, 
	'-' message_type, '-' message, 
	message_received, message_opened, link_tapped, 
	link_tapped_and_ordered, link_tapped_and_ordered_ltu, link_tapped_and_ordered_pu, link_tapped_and_ordered_su, link_tapped_and_ordered_3rau,  
	opened_via_notification, first_opened_via_notification
from 
	(select 
		count(distinct case when event_name like '%_message_received' then mobile_no else null end) message_received, 
		count(distinct case when event_name like '%_message_open' then mobile_no else null end) message_opened, 
		
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') then mobile_no else null end) link_tapped,
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in(select mobile_no from cjm_segmentation.retained_users where report_date=current_date-1 and tg in('LTUCb','LTUTa')) then mobile_no else null end) link_tapped_ltu, 
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in(select mobile_no from cjm_segmentation.retained_users where report_date=current_date-1 and tg in('PUCb','PU Set-A','PU Set-B','PU Set-C','PUTa','PUTa+Cb','PUTacs')) then mobile_no else null end) link_tapped_pu, 
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in(select mobile_no from cjm_segmentation.retained_users where report_date=current_date-1 and tg in('SPU')) then mobile_no else null end) link_tapped_su, 
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in(select mobile_no from cjm_segmentation.retained_users where report_date=current_date-1 and tg in('3RAUCb','3RAU Set-A','3RAU Set-B','3RAU Set-C','3RAUTa','3RAUTa+Cb','3RAUTacs')) then mobile_no else null end) link_tapped_3rau, 
		
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in (select right("ফোন"::text, 11) from data_vajapora.samsung_numbers) then mobile_no else null end) link_tapped_and_ordered,
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in (select right("ফোন"::text, 11) from data_vajapora.samsung_numbers) and mobile_no in(select mobile_no from cjm_segmentation.retained_users where report_date=current_date-1 and tg in('LTUCb','LTUTa')) then mobile_no else null end) link_tapped_and_ordered_ltu, 
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in (select right("ফোন"::text, 11) from data_vajapora.samsung_numbers) and mobile_no in(select mobile_no from cjm_segmentation.retained_users where report_date=current_date-1 and tg in('PUCb','PU Set-A','PU Set-B','PU Set-C','PUTa','PUTa+Cb','PUTacs')) then mobile_no else null end) link_tapped_and_ordered_pu, 
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in (select right("ফোন"::text, 11) from data_vajapora.samsung_numbers) and mobile_no in(select mobile_no from cjm_segmentation.retained_users where report_date=current_date-1 and tg in('SPU')) then mobile_no else null end) link_tapped_and_ordered_su, 
		count(distinct case when event_name in('in_app_message_link_tap', 'inbox_message_action') and mobile_no in (select right("ফোন"::text, 11) from data_vajapora.samsung_numbers) and mobile_no in(select mobile_no from cjm_segmentation.retained_users where report_date=current_date-1 and tg in('3RAUCb','3RAU Set-A','3RAU Set-B','3RAU Set-C','3RAUTa','3RAUTa+Cb','3RAUTacs')) then mobile_no else null end) link_tapped_and_ordered_3rau            
	from data_vajapora.help_b
	) tbl1, 
	
	(select 
		count(distinct mobile_no) opened_via_notification, 
		count(distinct case when id is not null then mobile_no else null end) first_opened_via_notification
	from tallykhata.mom_cjm_performance_detailed
	where (notification_id, bulk_notification_id) in(select notification_id, bulk_notification_id from data_vajapora.help_a)
	) tbl2; 
