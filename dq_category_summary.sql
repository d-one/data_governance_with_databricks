SELECT
	check_type,
	CASE WHEN val >= min_val and val <= max_val THEN "Pass" ELSE "Fail" END AS `Check Status`,
	COUNT(*) AS `Count`
FROM store_data_catalog.blog_post_store_transactions_db.transactions_raw_dq
GROUP BY check_type, `Check Status`
