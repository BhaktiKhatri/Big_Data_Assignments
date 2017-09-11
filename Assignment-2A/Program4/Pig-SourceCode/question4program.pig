A = load '/user/brk160030/business.csv' as line1;
B = foreach A generate FLATTEN((tuple(chararray,chararray,chararray))REGEX_EXTRACT_ALL(line1,'(.*)\\:\\:(.*)\\:\\:(.*)')) as (business_id,full_address,categories);
M = FILTER B BY (full_address MATCHES '.*Stanford.*');
C = load '/user/brk160030/review.csv' as line2;
D = foreach C generate FLATTEN((tuple(chararray,chararray,chararray,float))REGEX_EXTRACT_ALL(line2,'(.*)\\:\\:(.*)\\:\\:(.*)\\:\\:(.*)')) as (review_id,user_id,business_id,stars);
E = join M by business_id, D by business_id;
F = foreach E generate (D::user_id,D::stars);
G = limit F 10;
dump G;