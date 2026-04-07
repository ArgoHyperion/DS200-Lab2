
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
      FLATTEN(TOKENIZE(LOWER(review))) AS word;

joined = JOIN tok BY word LEFT OUTER, stop BY word;
clean = FILTER joined BY stop::word IS NULL;
words = FOREACH clean GENERATE tok::category AS category, tok::word AS word;

grp = GROUP words BY (category, word);
counts = FOREACH grp GENERATE
    group.category AS category,
    group.word AS word,
    COUNT(words) AS cnt;

grp2 = GROUP counts BY category;

top_words = FOREACH grp2 {
    ordered = ORDER counts BY cnt DESC;
    top5 = LIMIT ordered 5;
    GENERATE
        group AS category,
        FLATTEN(top5.(word, cnt));
};

STORE top_words INTO 'output_relevant_words';
