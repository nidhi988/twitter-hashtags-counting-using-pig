#using pig in hdfs mode
load_tweets=LOAD '/home/user/flume/tweets/get_tweets.csv' using PigStorage(',') AS myMap;

#checking to see that all tweets has been loaded successfully
dump load_tweets;


#extracting id and hashtags from above tweets
#hashtags are inside map 'entities' and extracting 'entities' as map[] data type

extract_details=FOREACH load_tweets GENERATE FLATTEN(myMap#'entities')as(m:map[]),FLATTEN (myMap#'id') as id;

describe extract_details;

#extracting hashtags
hash=FOREACH extract_details GENERATE FLATTEN(m#'hashtags')as(tags:map[]),id as id;

dump hash;

#extracting actual hashtag from text
txt=FOREACH hash GENERATE FLATTEN(tags#'text') as text,id;


grp=group txt by text;

count=FOREACH grp GENERATE group as hashtag_text,count(txt.text) as hashtag_count:int;

dump count;



