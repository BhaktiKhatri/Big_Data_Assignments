A = load '/user/brk160030/business.csv' as line1;
B = foreach A generate FLATTEN((tuple(chararray,chararray,chararray))REGEX_EXTRACT_ALL(line1,'(.*)\\:\\:(.*)\\:\\:(.*)')) as (business_id,full_address,categories);
C = load '/user/brk160030/review.csv' as line2;
D = foreach C generate FLATTEN((tuple(chararray,chararray,chararray,float))REGEX_EXTRACT_ALL(line2,'(.*)\\:\\:(.*)\\:\\:(.*)\\:\\:(.*)')) as (review_id,user_id,business_id,stars);
E = cogroup B by business_id, D by business_id;
F = limit E 5;
dump F;