raw = LOAD '/input/hotel-review.csv' USING PigStorage(';') AS 
(
  id:int, 
  review:chararray, 
  category:chararray, 
  aspect:chararray, 
  sentiment:chararray
);

filtered = FILTER raw BY sentiment == 'positive' OR sentiment == 'negative';
grp = GROUP filtered BY (sentiment, aspect);

counts = FOREACH grp GENERATE
    group.sentiment AS sentiment,
    group.aspect AS aspect,
    COUNT(filtered) AS cnt;

grp2 = GROUP counts BY sentiment;

top = FOREACH grp2 {
    top1 = TOP(1, 2, counts);
    GENERATE group AS sentiment, FLATTEN(top1.(aspect, cnt));
};

-- Tách output
top_pos = FILTER top BY sentiment == 'positive';
top_neg = FILTER top BY sentiment == 'negative';

STORE top_pos INTO 'output_top_positive' USING PigStorage(',');
STORE top_neg INTO 'output_top_negative' USING PigStorage(',');
