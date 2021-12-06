---------------------------------------------------------------------------------------------------------------------------------------
-- Q1) For a day, number of +, - or neutral weather status

select tweet_day 
	,sum(case when (tweet_sentiment) = 'positive' then 1 end) as positive
	,sum(case when (tweet_sentiment) = 'neutral' then 1 end) as neutral
	,sum(case when(tweet_sentiment) = 'negative' then 1 end) as negative
from warehouse.data_warehouse.sentiment_analysis sa 
group by tweet_day;

-- result

-- tweet_day |positive|neutral|negative|
-- ----------+--------+-------+--------+
-- 2021-11-25|      33|     52|      11|
-- 2021-11-21|      61|    107|      26|
-- 2021-11-26|      65|    106|      19|
-- 2021-11-29|      88|    157|      33|
-- 2021-11-22|      22|     61|      15|
-- ...       |     ...|    ...|     ...|



---------------------------------------------------------------------------------------------------------------------------------------
-- Q2) Weather status with the biggest impact in sentiment. Do for +, - and neutral
SELECT weather_status 
	,100 * sum(CASE when (tweet_sentiment) = 'positive' then 1 end) / count(tweet_sentiment) as percent_positive
	,100 * sum(case when (tweet_sentiment) = 'neutral' then 1 end) / count(tweet_sentiment) as percent_neutral
	,100 * sum(case when(tweet_sentiment) = 'negative' then 1 end) / count(tweet_sentiment) as percent_negative
FROM warehouse.data_warehouse.sentiment_analysis sa 
GROUP BY weather_status;

-- result

-- weather_status                                    |percent_positive|percent_neutral|percent_negative|
-- --------------------------------------------------+----------------+---------------+----------------+
-- clear sky                                         |              32|             57|              10|
-- broken clouds                                     |              31|             56|              12|
-- scattered clouds                                  |              29|             58|              11|
-- few clouds                                        |              31|             56|              12|



---------------------------------------------------------------------------------------------------------------------------------------
-- Q3) Most common words for +, - or neutral sentiment

-- TODO: how to keep the column tweet_sentiment in the ts_stat function? how to add it later?
-- TODO: how to skip hardcoding columns ?

WITH positive_tweets AS (  
	SELECT *
		,'positive' AS tweet_sentiment
	FROM ts_stat($$
		SELECT to_tsvector('ts.english_simple', tweet_text)
		FROM sentiment_analysis 
		WHERE (tweet_sentiment='positive') AND (tweet_text NOT LIKE '%http%' AND tweet_text NOT LIKE '%@%' AND LENGTH(BTRIM(tweet_text))>1)
	$$)
),

neutral_tweets AS (
	SELECT *
		,'neutral' AS tweet_sentiment
	FROM ts_stat($$
		SELECT to_tsvector('ts.english_simple', tweet_text)
		FROM sentiment_analysis 
		WHERE (tweet_sentiment='neutral') AND (tweet_text NOT LIKE '%http%' AND tweet_text NOT LIKE '%@%' AND LENGTH(BTRIM(tweet_text))>1)
	$$)
),

negative_tweets AS (
	SELECT *
		,'negative' AS tweet_sentiment
	FROM ts_stat($$
		SELECT to_tsvector('ts.english_simple', tweet_text)
		FROM sentiment_analysis 
		WHERE (tweet_sentiment='negative') AND (tweet_text NOT LIKE '%http%' AND tweet_text NOT LIKE '%@%' AND LENGTH(BTRIM(tweet_text))>1)
	$$)
),

union_all_tweets AS (
	SELECT word
		,nentry AS word_count
		,tweet_sentiment
	FROM (
		SELECT *
		FROM positive_tweets
		UNION ALL
		SELECT *
		FROM neutral_tweets
		UNION ALL 
		SELECT *
		FROM negative_tweets
		) AS all_tweets
),

spread_rows2cols AS (
	SELECT word
		,CASE WHEN tweet_sentiment = 'positive' THEN word_count END AS positive
		,CASE WHEN tweet_sentiment = 'neutral' THEN word_count END AS neutral
		,CASE WHEN tweet_sentiment = 'negative' THEN word_count END AS negative		
	FROM union_all_tweets
)


SELECT word
	,coalesce(sum(positive), 0) AS positive
	,coalesce(sum(neutral), 0) AS neutral
	,coalesce(sum(negative),0) AS negative
FROM spread_rows2cols
GROUP BY word
ORDER BY positive DESC 
LIMIT 50;

-- result 

-- word   |positive|neutral|negative|
-- -------+--------+-------+--------+
-- love   |      20|      1|       1|
-- good   |      16|      0|       2|
-- m      |      15|     14|      15|  -- TODO: why is not getting filtered?
-- like   |      11|     12|       7|
-- new    |       9|      0|       1|
-- right  |       7|      0|       1|
-- today  |       7|      2|       3|
-- one    |       7|      3|       4|
-- much   |       7|      0|       0|
-- still  |       7|      3|       2|
-- time   |       7|      4|       3|
-- best   |       6|      0|       0|
-- better |       6|      0|       0|
-- first  |       6|      1|       0|
-- go     |       6|      9|       4|
-- re     |       6|      2|       4|
-- need   |       6|     10|       4|
-- see    |       6|      7|       4|
-- thing  |       5|      1|       0|
-- let    |       5|      9|       0|
-- get    |       5|      6|       4|
-- away   |       5|      1|       0|
-- excited|       4|      0|       0|
-- really |       4|      0|       2|
-- people |       4|      1|       3|
-- alexia |       4|      1|       1|
-- lol    |       4|      0|       0|
-- players|       4|      0|       0|
-- want   |       4|      3|       2|
-- pretty |       4|      0|       1|
-- f1     |       3|      0|       1|
-- work   |       3|      2|       1|
-- take   |       3|      2|       1|
-- pedri  |       3|      0|       1|
-- ever   |       3|      1|       1|
-- variant|       3|      0|       0|
-- learn  |       3|      1|       0|
-- expect |       3|      0|       0|
-- feel   |       3|      4|       3|
-- hold   |       3|      1|       0|
-- wanna  |       3|      3|       0|
-- far    |       3|      0|       1|
-- going  |       3|      3|       2|
-- exactly|       3|      0|       0|
-- great  |       3|      0|       0|
-- way    |       3|      2|       3|
-- young  |       3|      0|       0|
-- think  |       3|      1|       1|
-- barca  |       3|      1|       1|
-- believe|       3|      1|       1|
