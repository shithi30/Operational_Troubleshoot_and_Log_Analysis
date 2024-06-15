/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=30784668
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

-- reg from Shareit
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select distinct mobile_no, min(reg_date) reg_date
from 
	(select mobile_no, advertise_id gaid
	from public.register_tallykhatauser
	) tbl1 
	
	inner join 
	
	(select gaid
	from data_vajapora.shareit_performance_tracking
	) tbl2 using(gaid) 
	
	inner join 
	
	(select mobile_number mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	) tbl3 using(mobile_no)
group by 1; 

-- biz type 
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
	
	(select 
		mobile mobile_no, 
		max(new_bi_business_type) new_bi_business_type
	from tallykhata.tallykhata_user_personal_info 
	group by 1
	) tbl3 using(mobile_no); 

-- all stats
drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
select * 
from 	
	(select mobile_number mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	where date(created_at)>'2021-11-30'::date
	) tbl0
	
	left join

	(select mobile_no, 1 if_shareit_reg
	from data_vajapora.help_a 
	where reg_date>'2021-11-30'::date
	) tbl1 using(mobile_no)
	
	left join 
	
	(select mobile_no, biz_type 
	from data_vajapora.help_b 
	) tbl2 using(mobile_no)
	
	left join 
	
	(select mobile mobile_no, max(district_name) district 
	from tallykhata.tallykhata_clients_location_info 
	group by 1 
	) tbl3 using(mobile_no)
	
	left join 
	
	(select mobile_no, min(created_datetime) fst_txn_date 
	from tallykhata.tallykhata_transacting_user_date_sequence_final 
	group by 1
	) tbl4 using(mobile_no)

	left join 
	
	(select 
		mobile_no,
		max(
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
			end
		) tg
	from cjm_segmentation.retained_users 
	where report_date=current_date
	group by 1
	) tbl5 using(mobile_no); 
	
-- summary (overall)
select left(reg_date::text, 7) reg_month, biz_type, count(mobile_no) merchants, count(mobile_no)*1.00/(select count(mobile_no) from data_vajapora.help_c) merchants_pct     
from data_vajapora.help_c 
group by 1, 2 
order by 3 desc; 

select left(reg_date::text, 7) reg_month, district, count(mobile_no) merchants, count(mobile_no)*1.00/(select count(mobile_no) from data_vajapora.help_c) merchants_pct
from data_vajapora.help_c 
group by 1, 2 
order by 3 desc; 

select left(reg_date::text, 7) reg_month, case when fst_txn_date is not null then 1 else 0 end if_transacted, count(mobile_no) merchants, count(mobile_no)*1.00/(select count(mobile_no) from data_vajapora.help_c) merchants_pct
from data_vajapora.help_c 
group by 1, 2 
order by 3 desc; 

select left(reg_date::text, 7) reg_month, tg, count(mobile_no) merchants, count(mobile_no)*1.00/(select count(mobile_no) from data_vajapora.help_c) merchants_pct
from data_vajapora.help_c 
group by 1, 2 
order by 3 desc; 

-- summary (Shareit)
select left(reg_date::text, 7) reg_month, biz_type, count(mobile_no) merchants, count(mobile_no)*1.00/(select count(mobile_no) from data_vajapora.help_c where if_shareit_reg=1) merchants_pct
from data_vajapora.help_c 
where if_shareit_reg=1
group by 1, 2 
order by 3 desc; 

select left(reg_date::text, 7) reg_month, district, count(mobile_no) merchants, count(mobile_no)*1.00/(select count(mobile_no) from data_vajapora.help_c where if_shareit_reg=1) merchants_pct
from data_vajapora.help_c 
where if_shareit_reg=1
group by 1, 2 
order by 3 desc; 

select left(reg_date::text, 7) reg_month, case when fst_txn_date is not null then 1 else 0 end if_transacted, count(mobile_no) merchants, count(mobile_no)*1.00/(select count(mobile_no) from data_vajapora.help_c where if_shareit_reg=1) merchants_pct                  
from data_vajapora.help_c 
where if_shareit_reg=1
group by 1, 2 
order by 3 desc; 

select left(reg_date::text, 7) reg_month, tg, count(mobile_no) merchants, count(mobile_no)*1.00/(select count(mobile_no) from data_vajapora.help_c where if_shareit_reg=1) merchants_pct                  
from data_vajapora.help_c 
where if_shareit_reg=1
group by 1, 2 
order by 3 desc; 