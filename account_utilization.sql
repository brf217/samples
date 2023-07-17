-----------------------------------------------------------------------------
---Date range of utilization and average monthly utilization by subscription
-----------------------------------------------------------------------------

WITH full_table AS (
	SELECT  
		facts.date_dimension_id, 
		facts.subscription_dimension_id, 
		subs.name,
		subs.is_active,
		subs.stack,
		subs.paid_seats,
		facts.monthly_perc_user_utilization,
		facts.weekly_perc_user_utilization,
		facts.daily_users,
		facts.health,facts.health2,
		facts.monthly_artifact_growth,
		AVG(monthly_perc_user_utilization) OVER (PARTITION BY subscription_dimension_id) avg_month_util_dtrange,
		AVG(weekly_perc_user_utilization) OVER (PARTITION BY subscription_dimension_id) avg_week_util_dtrange,
		(facts.monthly_perc_user_utilization - AVG(monthly_perc_user_utilization) OVER (PARTITION BY subscription_dimension_id)) month_avg_dif,
		(facts.weekly_perc_user_utilization - AVG(weekly_perc_user_utilization) OVER (PARTITION BY subscription_dimension_id)) wkly_avg_dif
	FROM 	
		daily_health_metrics_facts facts
	JOIN	
      		subscription_dimension subs 
		ON subs.id = facts.subscription_dimension_id
	WHERE 	
      		date_dimension_id BETWEEN '2018-04-30' AND '2018-06-01'
  		AND is_active = 't'
  		AND stack IN ('prod' , 'emea')
  		AND subscription_dimension_id IN (SELECT subscription_dimension_id
				  	FROM daily_health_metrics_facts
                                        WHERE date_dimension_id BETWEEN '2018-04-30' AND '2018-06-01'
                                        GROUP BY subscription_dimension_id
                                        HAVING COUNT(subscription_dimension_id) > 30)
	ORDER BY subscription_dimension_id, date_dimension_id DESC
	)
SELECT DISTINCT ON (subscription_dimension_id) * 
FROM full_table
ORDER BY subscription_dimension_id, date_dimension_id DESC
