SELECT 
	name AS `Check`,
	data_element AS `Data Element`,
	CASE WHEN val >= min_val and val <= max_val THEN "Pass" ELSE "Fail" END AS `Check Status`,
	val AS `Check Result`,
	min_val AS `Lower Limit`,
	max_val AS `Upper Limit`,
	description AS `Check Description`,
	info AS `Additional Information`
FROM store_data_catalog.blog_post_store_transactions_db.transactions_raw_dq
ORDER BY 
CASE WHEN `Check Status` = "Fail" THEN 1
              ELSE 2 END
