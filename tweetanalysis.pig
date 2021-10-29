demonet = load '/home/rishad/inputdata/pig/tweet/demonetization-tweets.csv' using org.apache.pig.piggybank.storage.CSVExcelStorage (',','YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') as (id:int,tweet:chararray);
dict = load 'file:///home/rishad/inputdata/pig/tweet/AFINN.txt' using PigStorage ('\t') as (word:chararray,rating:int);
tok = foreach demonet generate id,FLATTEN(TOKENIZE(tweet));
join_res = JOIN tok by token,dict by word;
grp = GROUP join_res by tok::id;
result = foreach grp generate group,SUM(join_res.dict::rating);
result = foreach grp generate group,SUM(join_res.dict::rating) as rate;
result_final = foreach result generate group,(rate>0?'POSITIVE TWEET':'NEGETIVE TWEET');
