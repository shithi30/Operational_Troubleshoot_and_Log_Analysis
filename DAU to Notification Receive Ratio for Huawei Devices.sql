/*
- Viz: https://docs.google.com/spreadsheets/d/1GhaSgdYm97a8N5IIK3KkeRcOGwic9xPB7Qf1QrenMxQ/edit#gid=154424273
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

do $$ 

declare 
	var_date date='2021-12-15'::date; 
begin
	raise notice 'New OP goes below:'; 
	loop 
		delete from data_vajapora.dau_to_receive_analysis 
		where date=var_date; 
	
		insert into data_vajapora.dau_to_receive_analysis
		select 
			var_date date, 
			count(dau_mobile_no) dau, 
			
			count(case when device_brand='samsung' then dau_mobile_no else null end) dau_merchants_samsung, 
			count(case when device_brand='vivo' then dau_mobile_no else null end) dau_merchants_vivo, 
			count(case when device_brand='oppo' then dau_mobile_no else null end) dau_merchants_oppo, 
			count(case when device_brand='xiaomi' then dau_mobile_no else null end) dau_merchants_xiaomi, 
			count(case when device_brand='huawei' then dau_mobile_no else null end) dau_merchants_huawei, 
			count(case when device_brand='symphony' then dau_mobile_no else null end) dau_merchants_symphony, 
			count(case when device_brand='realme' then dau_mobile_no else null end) dau_merchants_realme, 
			count(case when device_brand='redmi' then dau_mobile_no else null end) dau_merchants_redmi, 
			count(case when device_brand='itel' then dau_mobile_no else null end) dau_merchants_itel, 
			count(case when device_brand='walton' then dau_mobile_no else null end) dau_merchants_walton,
			count(case when device_brand is null then dau_mobile_no else null end) dau_merchants_others, 
			
			count(case when device_brand='samsung' then received_mobile_no else null end) received_merchants_samsung, 
			count(case when device_brand='vivo' then received_mobile_no else null end) received_merchants_vivo, 
			count(case when device_brand='oppo' then received_mobile_no else null end) received_merchants_oppo, 
			count(case when device_brand='xiaomi' then received_mobile_no else null end) received_merchants_xiaomi, 
			count(case when device_brand='huawei' then received_mobile_no else null end) received_merchants_huawei, 
			count(case when device_brand='symphony' then received_mobile_no else null end) received_merchants_symphony, 
			count(case when device_brand='realme' then received_mobile_no else null end) received_merchants_realme, 
			count(case when device_brand='redmi' then received_mobile_no else null end) received_merchants_redmi, 
			count(case when device_brand='itel' then received_mobile_no else null end) received_merchants_itel, 
			count(case when device_brand='walton' then received_mobile_no else null end) received_merchants_walton,
			count(case when device_brand is null then received_mobile_no else null end) received_merchants_others
		from 
			(-- DAU
			select mobile_no dau_mobile_no
			from tallykhata.tallykhata_fact_info_final 
			where created_datetime=var_date
			
			union 
			
			select mobile_no dau_mobile_no
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				date(created_date)=var_date
				and event_name not in('in_app_message_received','inbox_message_received')
				
			union 
				
			select ss.mobile_number dau_mobile_no
			from 
				public.user_summary as ss 
				left join 
				public.register_usermobile as i on ss.mobile_number = i.mobile_number
			where 
				i.mobile_number is null 
				and ss.created_at::date=var_date
			) tbl1 
			
			left join 
						
			(-- brand
			select mobile_no device_mobile_no, device_brand
			from 
				(select mobile mobile_no, max(id) id
				from public.registered_users 
				where device_status='active'
				group by 1
				) tbl1 
				
				inner join 
				
				(select 
					id, 
					case 
						when lower(device_brand)='samsung' then 'samsung'
						when lower(device_brand)='vivo' then 'vivo'
						when lower(device_brand)='oppo' then 'oppo'
						when lower(device_brand)='xiaomi' then 'xiaomi'
						when lower(device_brand)='huawei' then 'huawei'
						when lower(device_brand)='symphony' then 'symphony'
						when lower(device_brand)='realme' then 'realme'
						when lower(device_brand)='redmi' then 'redmi'
						when lower(device_brand)='itel' then 'itel'
						when lower(device_brand)='walton' then 'walton'
					end device_brand
				from public.registered_users 
				) tbl2 using(id)
			) tbl2 on(tbl1.dau_mobile_no=tbl2.device_mobile_no)
			
			left join 
		
			(-- received merchants
			select distinct mobile_no received_mobile_no
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_name='inbox_message_received'
				and event_date=var_date
			) tbl3 on(tbl1.dau_mobile_no=tbl3.received_mobile_no);
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date='2022-01-01'::date then exit; 
		end if; 
	end loop; 
end $$; 

select 
	date, 
	dau_merchants_samsung, received_merchants_samsung, received_merchants_samsung*1.00/dau_merchants_samsung received_merchants_samsung_dau_pct, 
	dau_merchants_vivo, received_merchants_vivo, received_merchants_vivo*1.00/dau_merchants_vivo received_merchants_vivo_dau_pct,
	dau_merchants_oppo, received_merchants_oppo, received_merchants_oppo*1.00/dau_merchants_oppo received_merchants_oppo_dau_pct,
	dau_merchants_xiaomi, received_merchants_xiaomi, received_merchants_xiaomi*1.00/dau_merchants_xiaomi received_merchants_xiaomi_dau_pct,
	dau_merchants_huawei, received_merchants_huawei, received_merchants_huawei*1.00/dau_merchants_huawei received_merchants_huawei_dau_pct,
	dau_merchants_symphony, received_merchants_symphony, received_merchants_symphony*1.00/dau_merchants_symphony received_merchants_symphony_dau_pct,
	dau_merchants_realme, received_merchants_realme, received_merchants_realme*1.00/dau_merchants_realme received_merchants_realme_dau_pct,
	dau_merchants_redmi, received_merchants_redmi, received_merchants_redmi*1.00/dau_merchants_redmi received_merchants_redmi_dau_pct,
	dau_merchants_itel, received_merchants_itel, received_merchants_itel*1.00/dau_merchants_itel received_merchants_itel_dau_pct,
	dau_merchants_walton, received_merchants_walton, received_merchants_walton*1.00/dau_merchants_walton received_merchants_walton_dau_pct,
	dau_merchants_others, received_merchants_others, received_merchants_others*1.00/dau_merchants_others received_merchants_others_dau_pct
from data_vajapora.dau_to_receive_analysis
order by 1 desc; 

