/*
- Viz: https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=1482753307
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

-- day-to-day results: version-01
do $$ 

declare 
	var_date date:='2021-11-20'::date;
begin 
	raise notice 'New OP goes below:';
	loop
		delete from data_vajapora.new_version_campaigns
		where campaign_date=var_date; 
	
		-- updated TGs' events, messages on campaign-date
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select *
		from 
			(-- message-events on the day
			select 
				mobile_no, 
				event_timestamp, 
				event_name, 
				notification_id, 
				case when event_name like '%in_app%' then 'inapp' else 'inbox' end modality
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_date=var_date 
				and event_name in('in_app_message_open', 'in_app_message_received', 'inbox_message_open', 'inbox_message_received')
			) tbl1 
			
			inner join 
			
			(-- messages in detail
			select id notification_id, title message_headline, created_at message_created_at
			from public.notification_pushmessage
			) tbl2 using(notification_id)
			
			inner join 
			
			(-- merchants in 4.0.1
			select mobile_no, app_version_name, update_or_reg_datetime
			from data_vajapora.version_wise_days
			where app_version_name='4.0.1' 
			) tbl3 using(mobile_no)
			
			inner join 
			
			(-- TG on the date of campaign
			select mobile_no, tg tg_on_campaign_day
			from cjm_segmentation.retained_users 
			where report_date=var_date 
			) tbl4 using(mobile_no)	
		-- making sure event is recorded after updating
		where event_timestamp>=update_or_reg_datetime; 
		
		-- necessary metrics
		insert into data_vajapora.new_version_campaigns
		select 
			campaign_date, 
			tg_on_campaign_day, 
			modality, 
			message_headline, 
			tg_size_on_campaign_day_401, 
			message_received, 
			message_opened
		from 
			(-- for inapp
			select 
				var_date campaign_date, 
				'inapp' modality,
				tg_on_campaign_day,
				message_headline,
				count(distinct case when event_name='in_app_message_received' then mobile_no else null end) message_received, 
				count(distinct case when event_name='in_app_message_open' then mobile_no else null end) message_opened 
			from 
				data_vajapora.help_a tbl1 
				inner join 
				(select mobile_no, message_headline
				from data_vajapora.help_a 
				where event_name='in_app_message_received'
				) tbl2 using(mobile_no, message_headline)
			where modality='inapp'
			group by 1, 2, 3, 4
			
			union all
			
			-- for inbox
			select 
				var_date campaign_date, 
				'inbox' modality,
				tg_on_campaign_day,
				message_headline,
				count(distinct case when event_name='inbox_message_received' then mobile_no else null end) message_received, 
				count(distinct case when event_name='inbox_message_open' then mobile_no else null end) message_opened 
			from 
				data_vajapora.help_a tbl1 
				inner join 
				(select mobile_no, message_headline
				from data_vajapora.help_a 
				where event_name='inbox_message_received'
				) tbl2 using(mobile_no, message_headline)
			where modality='inbox'
			group by 1, 2, 3, 4
			) tbl1 
			
			inner join 
			
			(-- updated TGs on the day of campaign
			select tg tg_on_campaign_day, count(mobile_no) tg_size_on_campaign_day_401
			from 
				(select mobile_no, tg
				from cjm_segmentation.retained_users 
				where report_date=var_date
				) tbl1 
				
				inner join 
				
				(select mobile_no, update_or_reg_datetime
				from data_vajapora.version_wise_days
				where 
					app_version_name='4.0.1' 
					and date(update_or_reg_datetime)<=var_date
				) tbl2 using(mobile_no)
			group by 1
			) tbl2 using(tg_on_campaign_day); 
	
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.new_version_campaigns; 

-- TG-wise top-05 inapp+inbox messages in a timeframe
select
	-- campaign_date, 
	tg_on_campaign_day, 
	modality, 
	message_headline, 
	tg_size_on_campaign_day_401, 
	message_received, 
	message_opened, 
	message_received_pct, 
	message_opened_pct, 
	seq popularity
from 
	(-- for inbox
	select *, row_number() over(partition by tg_on_campaign_day order by message_opened_pct desc, message_opened desc) seq 
	from 
		(select *, message_received*1.00/tg_size_on_campaign_day_401 message_received_pct, message_opened*1.00/tg_size_on_campaign_day_401 message_opened_pct
		from data_vajapora.new_version_campaigns
		where 
			modality='inbox'
			and campaign_date>='2021-11-24' and campaign_date<='2021-11-25'
		) tbl1 
		
	union all 
	
	-- for inapp
	select *, row_number() over(partition by tg_on_campaign_day order by message_opened_pct desc, message_opened desc) seq 
	from 
		(select *, message_received*1.00/tg_size_on_campaign_day_401 message_received_pct, message_opened*1.00/tg_size_on_campaign_day_401 message_opened_pct
		from data_vajapora.new_version_campaigns
		where 
			modality='inapp'
			and campaign_date>='2021-11-24' and campaign_date<='2021-11-25'
		) tbl1
	) tbl1 
where seq in(1, 2, 3, 4, 5); 

-- top-05 inapp+inbox messages in a timeframe
select *
from 
	(select *, row_number() over(partition by modality order by message_opened_pct desc, message_opened desc) seq 
	from 
		(select *, message_received*1.00/tg_size_on_campaign_day_401 message_received_pct, message_opened*1.00/tg_size_on_campaign_day_401 message_opened_pct
		from data_vajapora.new_version_campaigns
		where campaign_date>='2021-11-24' and campaign_date<='2021-11-25'
		) tbl1 
	) tbl1
where seq in(1, 2, 3, 4, 5); 



-- day-to-day results: version-02
do $$ 

declare 
	var_date date:='2021-11-26'::date;
begin 
	raise notice 'New OP goes below:';
	loop
		delete from data_vajapora.new_version_campaigns_2
		where campaign_date=var_date; 
	
		-- updated TGs' events, messages on campaign-date
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select *
		from 
			(-- message-events on the day
			select mobile_no, event_timestamp, event_name, notification_id
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_date=var_date 
				and event_name in('inbox_message_open', 'inbox_message_received')
			) tbl1 
		
			inner join 
			
			(-- update time to 4.0.1
			select mobile_no, update_or_reg_datetime
			from data_vajapora.version_wise_days
			where app_version_name='4.0.1' 
			) tbl3 using(mobile_no)
			
			inner join 
			
			(-- TG on the date of campaign
			select mobile_no, tg tg_on_campaign_day
			from cjm_segmentation.retained_users 
			where report_date=var_date 
			) tbl4 using(mobile_no)	
		-- making sure event is recorded after updating
		where event_timestamp>=update_or_reg_datetime; 
		
		-- necessary metrics
		insert into data_vajapora.new_version_campaigns_2
		select 
			campaign_date, 
			tg_on_campaign_day, 
			modality, 
			notification_id, 
			tg_size_on_campaign_day_401, 
			message_received, 
			message_opened
		from 
			(select 
				var_date campaign_date, 
				'inbox' modality,
				tg_on_campaign_day,
				notification_id,
				count(distinct case when event_name='inbox_message_received' then mobile_no else null end) message_received, 
				count(distinct case when event_name='inbox_message_open' then mobile_no else null end) message_opened 
			from 
				data_vajapora.help_a tbl1 
				inner join 
				(select mobile_no, notification_id
				from data_vajapora.help_a 
				where event_name='inbox_message_received'
				) tbl2 using(mobile_no, notification_id)
			group by 1, 2, 3, 4
			) tbl1 
			
			inner join 
			
			(-- updated TGs on the day of campaign
			select tg tg_on_campaign_day, count(mobile_no) tg_size_on_campaign_day_401
			from 
				(select mobile_no, tg
				from cjm_segmentation.retained_users 
				where 
					report_date='2021-11-26'::date
					and tg not in('NN2-6', 'NN1')
					
				union all
				
				select mobile_no, tg
				from cjm_segmentation.retained_users 
				where 
					report_date=var_date
					and tg in('NN2-6', 'NN1')
				) tbl1 
				
				inner join 
				
				(select mobile_no, update_or_reg_datetime
				from data_vajapora.version_wise_days
				where 
					app_version_name='4.0.1' 
					and date(update_or_reg_datetime)<=var_date
				) tbl2 using(mobile_no)
			group by 1
			) tbl2 using(tg_on_campaign_day); 
	
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select 
	campaign_date, 
	tg_on_campaign_day, 
	modality, 
	-- notification_id, 
	title message_headline,
	tg_size_on_campaign_day_401, 
	message_received, 
	message_opened
	-- message_received*1.00/tg_size_on_campaign_day_401 tg_received_pct
from 
	data_vajapora.new_version_campaigns_2 tbl1 
	inner join 
	(select id notification_id, title 
	from public.notification_pushmessage 
	) tbl2 using(notification_id)
where 
	message_received*1.00/tg_size_on_campaign_day_401>=0.60
	and message_received*1.00/tg_size_on_campaign_day_401<=1.00
order by campaign_date; 
	