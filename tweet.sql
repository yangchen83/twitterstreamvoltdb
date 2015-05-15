CREATE TABLE tweet
(
  tweetId	VARCHAR(20)	 NOT NULL,
  userId	VARCHAR(15)	 NOT NULL,
  userName	VARCHAR(150) NOT NULL,
  PRIMARY KEY(tweetId)
);
PARTITION TABLE tweet ON COLUMN tweetId;

CREATE TABLE hashtag
(
  tweetId	VARCHAR(20)	NOT NULL,
  hashtag	VARCHAR(150)	NOT NULL,
);
PARTITION TABLE hashtag ON COLUMN tweetId;

CREATE PROCEDURE tweetinsert PARTITION ON TABLE tweet COLUMN tweetId
   AS UPSERT INTO tweet (tweetId, userId, userName) VALUES (?, ?, ?);
   
CREATE PROCEDURE hashtaginsert PARTITION ON TABLE hashtag COLUMN tweetId
   AS INSERT INTO hashtag (tweetId, hashtag) VALUES (?, ?);
   
CREATE PROCEDURE hashtagcount AS
	SELECT distinct hashtag, count(hashtag) as Number
	FROM hashtag 
	GROUP BY hashtag 
	ORDER BY Number DESC
	LIMIT 10;
	
CREATE PROCEDURE mosttweetuser AS
	SELECT DISTINCT userName, count(userName) as Number
	FROM tweet
	GROUP BY userName
	ORDER BY Number DESC
	LIMIT 3;
	
CREATE PROCEDURE tweetcount AS
	SELECT count(*) FROM tweet;