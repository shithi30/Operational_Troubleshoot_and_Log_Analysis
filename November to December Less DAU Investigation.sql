-- In Dec we have 10% more downloads and 13% more registrations than Nov. However we do not see DAU growth. Where are we losing clients? 

select *, (reg_dec-reg_nov)*1.00/reg_nov reg_inc_pct, (download_dec-download_nov)*1.00/download_nov download_inc_pct
from 
	(select count(distinct mobile_number) reg_nov
	from public.register_usermobile 
	where 
		date_part('year', created_at)=2021 
		and date_part('month', created_at)=11
		and date_part('day', created_at)<22
	) tbl1,
	
	(select count(distinct mobile_number) reg_dec
	from public.register_usermobile 
	where 
		date_part('year', created_at)=2021 
		and date_part('month', created_at)=12
		and date_part('day', created_at)<22
	) tbl2,

	(select sum(download) download_nov
	from tallykhata.tk_daily_registration_download
	where 
		date_part('year', created_at)=2021 
		and date_part('month', created_at)=11
		and date_part('day', created_at)<22
	) tbl3,
	
	(select sum(download) download_dec
	from tallykhata.tk_daily_registration_download
	where 
		date_part('year', created_at)=2021 
		and date_part('month', created_at)=12
		and date_part('day', created_at)<22
	) tbl4; 

-- image investigation
select *
from 
	(select count(*) id_count_journal_media, count(distinct mobile_no) merchants_journal_media
	from public.journal_media 
	) tbl1, 
	
	(select count(*) id_count_account_media, count(distinct mobile_no) merchants_account_media
	from public.account_media 
	) tbl2; 


