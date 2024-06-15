/*
- Viz: https://docs.google.com/spreadsheets/d/1lHufXCN1pyBy4Ss7L9QC_TSTgBcWkPD8VIuTUu3tG_w/edit#gid=208636175
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

-- pushed to DWH via Python from: https://docs.google.com/spreadsheets/d/1lHufXCN1pyBy4Ss7L9QC_TSTgBcWkPD8VIuTUu3tG_w/edit#gid=1329681774
select *
from data_vajapora.release_campaign_for_txns; 
	
/* campaigns to analyze */
drop table if exists data_vajapora.campaign_help_a;
create table data_vajapora.campaign_help_a as
select request_id, campaign_id, channel, message_type, created_at, start_datetime, end_datetime, regexp_replace(message, E'[\\n\\r]+', ' ', 'g' ) message, row_number() over(order by request_id asc) seq
from 
    (select distinct request_id 
    from test.campaign_data_v3
    ) tbl1

    inner join

    (select 
        request_id,
        case when schedule_time is not null then schedule_time else created_at end start_datetime, 
        case when schedule_time is not null then schedule_time else created_at end+interval '12 hours' end_datetime
    from public.notification_bulknotificationsendrequest
    ) tbl2 using(request_id)

    inner join 

    (select title campaign_id, id request_id, created_at, message_id
    from public.notification_bulknotificationrequest
    ) tbl3 using(request_id)

    inner join 

    (select "Channel" channel, "Campaign ID" campaign_id, "Message Type" message_type
    from data_vajapora.release_campaign_for_txns
    ) tbl4 using(campaign_id)

    left join 

    (select id message_id, summary message
    from public.notification_pushmessage
    ) tbl5 using(message_id)
where channel='Portal Inbox'; 

select *
from data_vajapora.campaign_help_a; 

/* feasibility of analysis */

-- txn specific campaigns launched: 150
select distinct "Campaign ID" 
from data_vajapora.release_campaign_for_txns; 

-- TG found for: 127 campaigns
select distinct campaign_id
from data_vajapora.campaign_help_a
where campaign_id in(select distinct "Campaign ID" from data_vajapora.release_campaign_for_txns); 

-- TG not found for: 23 campaigns 
select distinct "Campaign ID" 
from data_vajapora.release_campaign_for_txns
where "Campaign ID" not in(select distinct campaign_id from data_vajapora.campaign_help_a); 

-- channel found for all these 23 campaigns
select distinct campaign_id_title campaign_id
from test.campaign_info
where campaign_id_title in 
	(-- TG not found for: 23 campaigns 
	select distinct "Campaign ID" 
	from data_vajapora.release_campaign_for_txns
	where "Campaign ID" not in(select distinct campaign_id from data_vajapora.campaign_help_a)
	); 

-- 9 out of these 23 campaigns were created
select title campaign_id, id request_id, created_at
from public.notification_bulknotificationrequest
where title in 
	(-- TG not found for: 23 campaigns
	select distinct "Campaign ID" 
	from data_vajapora.release_campaign_for_txns
	where "Campaign ID" not in(select distinct campaign_id from data_vajapora.campaign_help_a)
	); 

/* the analyses */

-- Malik Dilo-Nilo
select 
	'Malik Dilo-Nilo' feature, 
	count(distinct campaign_id) campaigns,
	count(distinct tbl2.mobile_no) tg_size,
	count(distinct tbl3.mobile_no) merchants_clicked,
	count(distinct case when tbl3.mobile_no=tbl4.mobile_no then tbl4.mobile_no else null end) merchants_transacted_from_clicked,
	count(distinct tbl4.mobile_no) merchants_transacted_from_tg
from
	(select request_id, campaign_id, start_datetime, end_datetime, date(start_datetime) campaign_date
	from data_vajapora.campaign_help_a
	where message_type='Malik Dilo-Nilo'
	) tbl1 
	
	inner join 
	
	test.campaign_data_v3 tbl2 using(request_id)
	
	left join 
	
	(select mobile_no, event_timestamp
	from tallykhata.tallykhata_sync_event_fact_final
	where event_name='inbox_message_open'
	) tbl3 on(tbl2.mobile_no=tbl3.mobile_no and tbl3.event_timestamp>=tbl1.start_datetime and tbl3.event_timestamp<=tbl1.end_datetime)
	
	left join 
	
	(select mobile_no, created_datetime
	from tallykhata.tallykhata_fact_info_final
	where txn_type in('MALIK_DILO', 'MALIK_NILO')
	) tbl4 on(tbl2.mobile_no=tbl4.mobile_no and created_datetime=campaign_date); 

-- Cashbox Milai
select 
	'Cashbox Milai' feature, 
	count(distinct campaign_id) campaigns,
	count(distinct tbl2.mobile_no) tg_size,
	count(distinct tbl3.mobile_no) merchants_clicked,
	count(distinct case when tbl3.mobile_no=tbl4.mobile_no then tbl4.mobile_no else null end) merchants_transacted_from_clicked,
	count(distinct tbl4.mobile_no) merchants_transacted_from_tg
from
	(select request_id, campaign_id, start_datetime, end_datetime, date(start_datetime) campaign_date
	from data_vajapora.campaign_help_a
	where message_type='Cashbox Milai'
	) tbl1 
	
	inner join 
	
	test.campaign_data_v3 tbl2 using(request_id)
	
	left join 
	
	(select mobile_no, event_timestamp
	from tallykhata.tallykhata_sync_event_fact_final
	where event_name='inbox_message_open'
	) tbl3 on(tbl2.mobile_no=tbl3.mobile_no and tbl3.event_timestamp>=tbl1.start_datetime and tbl3.event_timestamp<=tbl1.end_datetime)
	
	left join 
	
	(select mobile_no, create_date::date
	from public.cashbox_adjustment
	) tbl4 on(tbl2.mobile_no=tbl4.mobile_no and create_date=campaign_date); 

-- Cash Becha
select 
	'Cash Becha' feature, 
	count(distinct campaign_id) campaigns,
	count(distinct tbl2.mobile_no) tg_size,
	count(distinct tbl3.mobile_no) merchants_clicked,
	count(distinct case when tbl3.mobile_no=tbl4.mobile_no then tbl4.mobile_no else null end) merchants_transacted_from_clicked,
	count(distinct tbl4.mobile_no) merchants_transacted_from_tg
from
	(select request_id, campaign_id, start_datetime, end_datetime, date(start_datetime) campaign_date
	from data_vajapora.campaign_help_a
	where message_type='Cash Becha'
	) tbl1 
	
	inner join 
	
	test.campaign_data_v3 tbl2 using(request_id)
	
	left join 
	
	(select mobile_no, event_timestamp
	from tallykhata.tallykhata_sync_event_fact_final
	where event_name='inbox_message_open'
	) tbl3 on(tbl2.mobile_no=tbl3.mobile_no and tbl3.event_timestamp>=tbl1.start_datetime and tbl3.event_timestamp<=tbl1.end_datetime)
	
	left join 
	
	(select mobile_no, created_datetime
	from tallykhata.tallykhata_fact_info_final
	where txn_type in('CASH_SALE')
	) tbl4 on(tbl2.mobile_no=tbl4.mobile_no and created_datetime=campaign_date); 

-- Cash Kena
select 
	'Cash Kena' feature, 
	count(distinct campaign_id) campaigns,
	count(distinct tbl2.mobile_no) tg_size,
	count(distinct tbl3.mobile_no) merchants_clicked,
	count(distinct case when tbl3.mobile_no=tbl4.mobile_no then tbl4.mobile_no else null end) merchants_transacted_from_clicked,
	count(distinct tbl4.mobile_no) merchants_transacted_from_tg
from
	(select request_id, campaign_id, start_datetime, end_datetime, date(start_datetime) campaign_date
	from data_vajapora.campaign_help_a
	where message_type='Cash Kena'
	) tbl1 
	
	inner join 
	
	test.campaign_data_v3 tbl2 using(request_id)
	
	left join 
	
	(select mobile_no, event_timestamp
	from tallykhata.tallykhata_sync_event_fact_final
	where event_name='inbox_message_open'
	) tbl3 on(tbl2.mobile_no=tbl3.mobile_no and tbl3.event_timestamp>=tbl1.start_datetime and tbl3.event_timestamp<=tbl1.end_datetime)
	
	left join 
	
	(select mobile_no, created_datetime
	from tallykhata.tallykhata_fact_info_final
	where txn_type in('CASH_PURCHASE')
	) tbl4 on(tbl2.mobile_no=tbl4.mobile_no and created_datetime=campaign_date); 
	