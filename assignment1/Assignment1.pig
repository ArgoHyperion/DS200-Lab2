
raw = LOAD '/input/hotel-review.csv'
      USING PigStorage(';')
      AS (id:int, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

stop = LOAD '/input/stopwords.txt'
       AS (word:chararray);

tok = FOREACH raw GENERATE
      id,
      FLATTEN(TOKENIZE(LOWER(review))) AS word;

joined = JOIN tok BY word LEFT OUTER, stop BY word;
clean = FILTER joined BY stop::word IS NULL;

result = FOREACH clean GENERATE tok::id, tok::word;
sorted = ORDER result BY id ASC;

STORE sorted INTO 'output'
      USING PigStorage(',');

