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
	Merchant name is one thing and shop name is another. 
	I can see 85% registered with shop names, while just 9% entered merchant names. 
*/

select 
	count(tbl1.mobile_no) reg_merchants, 
	count(tbl2.mobile_no) reg_named_merchants, 
	count(tbl2.mobile_no)*1.00/count(tbl1.mobile_no) reg_named_merchants_pct
from 
	(select mobile_number mobile_no 
	from public.register_usermobile
	) tbl1 
	
	left join 
	
	(select mobile_no
	from public.register_tallykhatauser
	where merchant_name is not null and merchant_name!=''
	-- where shop_name is not null and shop_name!=''
	) tbl2 using(mobile_no); 