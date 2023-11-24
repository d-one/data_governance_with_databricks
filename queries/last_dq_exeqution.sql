SELECT 
	MAX(`Timestamp`) AS `Checks last updated on`,
	COUNT(*) AS `Number of checks run`,
	SUM(CASE WHEN val >= min_val and val <= max_val THEN 0 ELSE 1 END) AS `Number of failed checks`
FROM store_data_catalog.blog_post_store_transactions_db.transactions_raw_dq
