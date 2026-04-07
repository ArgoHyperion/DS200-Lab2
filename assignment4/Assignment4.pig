
raw = LOAD '/input/hotel-review.csv' USING PigStorage(';') AS 
(
  id:int, 
  review:chararray, 
  category:chararray, 
  aspect:chararray, 
  sentiment:chararray
);

stop = LOAD '/input/stopwords.txt'
       AS (word:chararray);

tok = FOREACH raw GENERATE
      category,
      sentiment,
      FLATTEN(TOKENIZE(LOWER(review))) AS word;

joined = JOIN tok BY word LEFT OUTER, stop BY word;
clean = FILTER joined BY stop::word IS NULL;
words = FOREACH clean GENERATE tok::category AS category, tok::sentiment AS sentiment, tok::word AS word;


filtered = FILTER words BY sentiment == 'positive' OR sentiment == 'negative';

grp = GROUP filtered BY (category, sentiment, word);

counts = FOREACH grp GENERATE
    group.category AS category,
    group.sentiment AS sentiment,
    group.word AS word,
    COUNT(filtered) AS cnt;


grp2 = GROUP counts BY (category, sentiment);

top_words = FOREACH grp2 {
    ordered = ORDER counts BY cnt DESC;
    top5 = LIMIT ordered 5;
    GENERATE
        group.category AS category,
        group.sentiment AS sentiment,
        FLATTEN(top5.(word, cnt));
};


top_pos = FILTER top_words BY sentiment == 'positive';
top_neg = FILTER top_words BY sentiment == 'negative';

STORE top_pos INTO 'output_positive_words' USING PigStorage(',');
STORE top_neg INTO 'output_negative_words' USING PigStorage(',');
