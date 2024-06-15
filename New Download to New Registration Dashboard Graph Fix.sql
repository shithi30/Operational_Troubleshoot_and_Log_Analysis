/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=478721126
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
	- function: tallykhata.fn_daily_download_to_reg_ratios()
	- table: tallykhata.daily_download_to_reg_ratios
*/

-- previous
select * 
from 
	tallykhata.tk_daily_registration_download tbl1 
	
	left join 
	
	(select date_time created_at, uninstall 
	from tallykhata.tallykhata_playstore_installs_dashboard
	) tbl2 using(created_at); 

-- now 
drop table if exists tallykhata.daily_download_to_reg_ratios; 
create table tallykhata.daily_download_to_reg_ratios as
select 
	*, 
	case 
		when number_of_new_download=0 then null
		else newly_downloaded_regs/number_of_new_download
	end newly_downloaded_regs_pct
from 
	tallykhata.tk_daily_registration_download tbl1 
	
	left join 
	
	(select date_time created_at, uninstall 
	from tallykhata.tallykhata_playstore_installs_dashboard
	) tbl2 using(created_at)

	left join 
	
	(select 
		download_date created_at, 
		count(reg_ad_id) newly_downloaded_regs
	from 
		(select advertise_id download_ad_id, min(date(time)) download_date
		from public.google_ad_id
		group by 1
		) tbl1 
		
		inner join 
			
		(select advertise_id reg_ad_id, min(date(created_at)) reg_date
		from public.register_tallykhatauser
		where mobile_no is not null 
		group by 1
		) tbl2 on(tbl1.download_date=tbl2.reg_date and tbl1.download_ad_id=tbl2.reg_ad_id)
	group by 1
	) tbl3 using(created_at); 

select * 
from tallykhata.daily_download_to_reg_ratios; 
