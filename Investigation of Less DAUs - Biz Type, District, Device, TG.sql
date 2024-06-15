/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1957829120
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=602752640
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
	var_date date:=current_date-30; 

begin  
	raise notice 'New OP goes below:'; 

	-- biz-types (Mahmud)
	drop table if exists data_vajapora.help_b; 
	create table data_vajapora.help_b as
	select 
		mobile_no, 
		case 
			when business_type in('BAKERY_AND_CONFECTIONERY') then 'SWEETS AND CONFECTIONARY'
			when business_type in('ELECTRONICS') then 'ELECTRONICS STORE'
			when business_type in('MFS_AGENT','MFS_MOBILE_RECHARGE') then 'MFS-MOBILE RECHARGE STORE'
			when business_type in('GROCERY') then 'GROCERY'
			when business_type in('DISTRIBUTOR_OR_WHOLESALE','WHOLESALER','DEALER') then 'OTHER WHOLESELLER'
			when business_type in('HOUSEHOLD_AND_FURNITURE') then 'FURNITURE SHOP'
			when business_type in('STATIONERY') then 'STATIONARY BUSINESS'
			when business_type in('TAILORS') then 'TAILERS'
			when business_type in('PHARMACY') then 'PHARMACY'
			when business_type in('SHOE_STORE') then 'SHOE STORE'
			when business_type in('MOTOR_REPAIR') then 'VEHICLE-CAR SERVICING'
			when business_type in('COSMETICS') then 'COSMETICS AND PERLOUR'
			when business_type in('ROD_CEMENT') then 'CONSTRUCTION RAW MATERIAL'
			when business_type='' then upper(case when new_bi_business_type!='Other Business' then new_bi_business_type else null end) 
			else null 
		end biz_type
	from 
		(select id, business_type 
		from public.register_tallykhatauser 
		) tbl1 
		
		inner join 
		
		(select mobile_no, max(id) id
		from public.register_tallykhatauser 
		group by 1
		) tbl2 using(id)
		
		inner join 
		
		(select mobile mobile_no, max(new_bi_business_type) new_bi_business_type
		from tallykhata.tallykhata_user_personal_info 
		group by 1
		) tbl3 using(mobile_no); 
	
	-- district
	drop table if exists data_vajapora.help_c; 
	create table data_vajapora.help_c as
	select mobile_no, district_name 
	from 
		(select mobile mobile_no, max(id) id
		from tallykhata.tallykhata_clients_location_info
		group by 1
		) tbl1 
		
		inner join 
		
		(select id, district_name 
		from tallykhata.tallykhata_clients_location_info
		) tbl2 using(id); 
	
	-- device
	drop table if exists data_vajapora.help_d; 
	create table data_vajapora.help_d as
	select mobile_no, lower(device_brand) device_brand 
	from 
		(select mobile mobile_no, max(id) id
		from public.registered_users
		group by 1
		) tbl1 
		
		inner join 
		
		(select device_brand, id
		from public.registered_users
		) tbl2 using(id); 
	
	loop
		delete from data_vajapora.less_dau_distrib 
		where report_date=var_date; 
	
		-- TG
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select 
			mobile_no, 
			case 
				when tg like '3RAU%' then '3RAU'
				when tg like 'LTU%' then 'LTU'
				when tg like 'PU%' then 'PU'
				when tg like 'Z%' then 'Zombie' 
				when tg in('NT--') then 'NT'
				when tg in('NB0','NN1','NN2-6') then 'NN'
				when tg in('PSU') then 'PSU'
				when tg in('SPU') then 'SU'
				else null
			end tg
		from 
			(select mobile_no, max(tg) tg
			from cjm_segmentation.retained_users
			where report_date=var_date
			group by 1
			) tbl1; 
	
		-- DAUs 
		drop table if exists data_vajapora.help_f; 
		create table data_vajapora.help_f as
		
		select mobile_no, var_date report_date
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime=var_date
		
		union 
		
		select mobile_no, var_date report_date
		from tallykhata.tallykhata_sync_event_fact_final 
		where 
			event_date=var_date
			and event_name not in ('in_app_message_received','inbox_message_received')
			
		union 
			
		select ss.mobile_number mobile_no, var_date report_date
		from 
			public.user_summary as ss 
			left join 
			public.register_usermobile as i on ss.mobile_number = i.mobile_number
		where 
			i.mobile_number is null 
			and ss.created_at::date=var_date; 
		
		insert into data_vajapora.less_dau_distrib
		select * 
		from 
			data_vajapora.help_f tbl1
			left join 
			data_vajapora.help_a tbl2 using(mobile_no)
			left join 
			data_vajapora.help_b tbl3 using(mobile_no)
			left join 
			data_vajapora.help_c tbl4 using(mobile_no)
			left join 
			data_vajapora.help_d tbl5 using(mobile_no); 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.less_dau_distrib
limit 5000; 

-- TG
select 
	report_date,
	case when tg is not null then tg else 'others' end tg,
	count(mobile_no) daus
from data_vajapora.less_dau_distrib 
group by 1, 2
order by 1, 3; 

-- biz type
select 
	report_date,
	case 
		when biz_type in 
			(select biz_type 
			from 
				(select biz_type, count(*) 
				from data_vajapora.less_dau_distrib 
				where 
					report_date=(select max(report_date) from data_vajapora.less_dau_distrib)
					and biz_type is not null
				group by 1 
				order by 2 desc 
				limit 10
				) tbl1
			) 
			then biz_type 
		else 'others' 
	end biz_type,
	count(mobile_no) daus
from data_vajapora.less_dau_distrib 
group by 1, 2
order by 1, 3; 

-- district 
select 
	report_date,
	case 
		when district_name in 
			(select district_name 
			from 
				(select district_name, count(*) 
				from data_vajapora.less_dau_distrib 
				where 
					report_date=(select max(report_date) from data_vajapora.less_dau_distrib)
					and district_name is not null
				group by 1 
				order by 2 desc 
				limit 10
				) tbl1
			) 
			then district_name 
		else 'others' 
	end district_name,
	count(mobile_no) daus
from data_vajapora.less_dau_distrib 
group by 1, 2
order by 1, 3; 

-- device brand
select 
	report_date,
	case 
		when device_brand in 
			(select device_brand 
			from 
				(select device_brand, count(*) 
				from data_vajapora.less_dau_distrib 
				where 
					report_date=(select max(report_date) from data_vajapora.less_dau_distrib)
					and device_brand is not null
				group by 1 
				order by 2 desc 
				limit 10
				) tbl1
			) 
			then device_brand 
		else 'others' 
	end device_brand,
	count(mobile_no) daus
from data_vajapora.less_dau_distrib 
group by 1, 2
order by 1, 3; 
