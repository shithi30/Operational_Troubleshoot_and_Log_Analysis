/*
- Viz: 
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: https://datastudio.google.com/u/0/reporting/28d75b3f-3853-4fe0-8440-279eaa6c0e66/page/FajmB
- Email thread: 
- Notes (if any): 
*/

select * 
from 
	tallykhata.tk_daily_registration_download tbl1 
	
	inner join 
	
	(select date_time created_at, uninstall 
	from tallykhata.tallykhata_playstore_installs_dashboard
	) tbl2 using(created_at); 