
raw = LOAD '/input/hotel-review.csv' USING PigStorage(';') AS (
    id:int, 
    review:chararray, 
    category:chararray, 
    aspect:chararray, 
    sentiment:chararray
);
-- Count word
stop = LOAD '/input/stopwords.txt' AS (word:chararray);

tok = FOREACH raw GENERATE
      id,
      FLATTEN(TOKENIZE(LOWER(review))) AS word;
joined = JOIN tok BY word LEFT OUTER, stop BY word;
clean = FILTER joined BY stop::word IS NULL;
word_groups = GROUP clean BY tok::word;
word_count = FOREACH word_groups GENERATE group AS word, COUNT(clean) AS frequency;
popular_words = FILTER word_count BY frequency > 500;

STORE popular_words INTO 'output_popular' USING PigStorage(',');

-- Count category
category_distinct = DISTINCT (FOREACH raw GENERATE id, category);
category_groups = GROUP category_distinct BY category;
category_count = FOREACH category_groups GENERATE group AS category, COUNT(category_distinct) AS num_reviews;

STORE category_count INTO 'output_catergory' USING PigStorage(',');

-- Count aspect
aspect_distinct = DISTINCT (FOREACH raw GENERATE id, aspect);
aspect_groups = GROUP aspect_distinct BY aspect;
aspect_count = FOREACH aspect_groups GENERATE group AS aspect, COUNT(aspect_distinct) AS num_reviews;

STORE aspect_count INTO 'output_aspect' USING PigStorage(',');

