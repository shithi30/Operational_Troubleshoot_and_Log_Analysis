/*
- Viz: 
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
merchant: 01632863848
আরাফাত নামে আমার টালিখাতায় নেই। তবু মেসেজ  আসে কেন!
*/

-- giving different results in live and DWH, reason: name-changes not synced in public.account, will now be synced
select *
from 
	(select mobile_no, contact, max(id) account_id
	from public.account 
	where 
		type=2
		and mobile_no='01632863848'
	group by 1, 2
	) tbl1 
	
	inner join 
	
	(select id account_id, name
	from public.account  
	) tbl2 using(account_id) 
where name='আরাফাত'; 
