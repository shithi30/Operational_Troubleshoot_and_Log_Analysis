/*
- Cal: https://docs.google.com/spreadsheets/d/1LtrEpjUcYbBToRkR8FWC7EPnj6Xh7mek2LWiH_rV4II/edit#gid=956470178
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1kgO7_kkg5CFwRy4GNHLTnBT7ssTyhdygsFi5gH3Cduc/edit#gid=263632049
- Table:
- File: 
- Email thread: 
- Notes (if any): 
for Inbox, run: 24-May-21.ipynb (previously 23-Mar-21 (4).ipynb)
for Inaapp, run: 24-Mar-21 (1).ipynb
*/

/* for Inbox */
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select 
	campaign_id, start_date, tg_set, channel, 
	tg_size, ibx_open_users, avg_view_time_sec, 
	ibx_open_plus_20_mins_trt, ibx_open_plus_20_mins_trt_users, 
	ibx_open_plus_20_mins_trt/(ibx_open_plus_20_mins_trt_users*20.00) ibx_open_plus_20_mins_trt_rate,
	other_hrs_trt, other_hrs_users,
	other_hrs_trt/(other_hrs_users*120.00) ibx_open_plus_120_mins_trt_rate,
	(ibx_open_plus_20_mins_trt/(ibx_open_plus_20_mins_trt_users*20.00))/(other_hrs_trt/(other_hrs_users*120.00)) growth_rate,
	message, 
	request_id
from 
	(select 
		request_id, 
		ibx_open_users, avg_view_time_sec, 
		ibx_open_plus_20_mins_trt, ibx_open_plus_20_mins_trt_users, 
		other_hrs_trt, other_hrs_users
	from tallykhata.inbox_5_10_20_60_mins_analysis
	) tbl1 
	
	inner join 
	
	(select request_id, campaign_id, tg_set, channel, message, min(start_date) start_date
	from data_vajapora.analyzable_inbox_campaigns
	group by 1, 2, 3, 4, 5
	) tbl2 using(request_id)
	
	inner join 
	
	(select request_id, count(mobile) tg_size
	from public.notification_bulknotificationreceiver
	group by 1 
	) tbl3 using(request_id)
where 	
	ibx_open_plus_20_mins_trt_users!=0
	and other_hrs_users!=0;

-- all mature campaigns validly (excluding div by 0 cases) analyzed
select 
	campaign_id, start_date, tg_set, channel, 
	tg_size, ibx_open_users, avg_view_time_sec, 
	ibx_open_plus_20_mins_trt, ibx_open_plus_20_mins_trt_users, 
	ibx_open_plus_20_mins_trt_rate,
	other_hrs_trt, other_hrs_users,
	ibx_open_plus_120_mins_trt_rate,
	growth_rate,
	regexp_replace(message, E'[\\n\\r]+', ' ', 'g' ) message
from data_vajapora.help_a tbl1
order by growth_rate desc;

-- mature campaigns validly (excluding div by 0 cases) analyzed, for L campaigns
select 
	campaign_id, start_date, tg_set, channel, 
	tg_size, ibx_open_users, avg_view_time_sec, 
	ibx_open_plus_20_mins_trt, ibx_open_plus_20_mins_trt_users, 
	ibx_open_plus_20_mins_trt_rate,
	other_hrs_trt, other_hrs_users,
	ibx_open_plus_120_mins_trt_rate,
	growth_rate,
	regexp_replace(message, E'[\\n\\r]+', ' ', 'g' ) message
from data_vajapora.help_a
where campaign_id like 'L%' 
order by growth_rate desc;

-- total camapigns launched, for L campaigns
select count(distinct request_id)
from data_vajapora.analyzable_inbox_campaigns
where campaign_id like 'L%';

-- matured campaigns, for L campaigns
select count(distinct request_id)
from data_vajapora.analyzable_inbox_campaigns
where 
	date(end_datetime)<current_date-3
	and campaign_id like 'L%';

-- results available for (unavailability may be caused due to div by 0), for L campaigns
select count(distinct request_id)
from data_vajapora.help_a
where campaign_id like 'L%';

-- div by 0 cases: all cases, for L campaigns
select campaign_id, tbl1.*
from 
	(select *
	from tallykhata.inbox_5_10_20_60_mins_analysis
	) tbl1 
	
	left join 
	
	(select distinct request_id 
	from data_vajapora.help_a
	) tbl2 using(request_id)
	
	left join 
	
	(select distinct request_id, campaign_id
	from data_vajapora.analyzable_inbox_campaigns
	) tbl3 using(request_id)
where 
	tbl2.request_id is null
	and campaign_id like 'L%' -- comment this out for all campaigns
order by request_id; 




/* for Inapp */
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select 
	campaign_id, start_date, tg_set, channel, 
	tg_size, iap_open_users, avg_view_time_sec, 
	iap_open_plus_20_mins_trt, iap_open_plus_20_mins_trt_users, 
	iap_open_plus_20_mins_trt/(iap_open_plus_20_mins_trt_users*20.00) iap_open_plus_20_mins_trt_rate,
	other_hrs_trt, other_hrs_users,
	other_hrs_trt/(other_hrs_users*120.00) iap_open_plus_120_mins_trt_rate,
	(iap_open_plus_20_mins_trt/(iap_open_plus_20_mins_trt_users*20.00))/(other_hrs_trt/(other_hrs_users*120.00)) growth_rate,
	message, 
	request_id
from 
	(select 
		request_id, 
		iap_open_users, avg_view_time_sec, 
		iap_open_plus_20_mins_trt, iap_open_plus_20_mins_trt_users, 
		other_hrs_trt, other_hrs_users
	from tallykhata.inapp_5_10_20_60_mins_analysis
	) tbl1 
	
	inner join 
	
	(select request_id, campaign_id, tg_set, channel, message, min(start_date) start_date
	from data_vajapora.analyzable_inapp_campaigns
	group by 1, 2, 3, 4, 5
	) tbl2 using(request_id)
	
	inner join 
	
	(select request_id, count(mobile) tg_size
	from public.notification_bulknotificationreceiver
	group by 1 
	) tbl3 using(request_id)
where 	
	iap_open_plus_20_mins_trt_users!=0
	and other_hrs_users!=0;

-- all mature campaigns validly (excluding div by 0 cases) analyzed
select 
	campaign_id, start_date, tg_set, channel, 
	tg_size, iap_open_users, avg_view_time_sec, 
	iap_open_plus_20_mins_trt, iap_open_plus_20_mins_trt_users, 
	iap_open_plus_20_mins_trt_rate,
	other_hrs_trt, other_hrs_users,
	iap_open_plus_120_mins_trt_rate,
	growth_rate,
	regexp_replace(message, E'[\\n\\r]+', ' ', 'g' ) message
from data_vajapora.help_a
order by growth_rate desc;

-- mature campaigns validly (excluding div by 0 cases) analyzed, for L campaigns
select 
	campaign_id, start_date, tg_set, channel, 
	tg_size, iap_open_users, avg_view_time_sec, 
	iap_open_plus_20_mins_trt, iap_open_plus_20_mins_trt_users, 
	iap_open_plus_20_mins_trt_rate,
	other_hrs_trt, other_hrs_users,
	iap_open_plus_120_mins_trt_rate,
	growth_rate,
	regexp_replace(message, E'[\\n\\r]+', ' ', 'g' ) message
from data_vajapora.help_a
where campaign_id like 'L%'
order by growth_rate desc;

-- total camapigns launched, for L campaigns
select count(distinct request_id)
from data_vajapora.analyzable_inapp_campaigns
where campaign_id like 'L%';

-- matured campaigns, for L campaigns
select count(distinct request_id)
from data_vajapora.analyzable_inapp_campaigns
where 
	date(end_datetime)<current_date-3
	and campaign_id like 'L%';

-- results available for (unavailability may be caused due to div by 0), for L campaigns
select count(distinct request_id)
from data_vajapora.help_a
where campaign_id like 'L%';
	