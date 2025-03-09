-- alter table 'sales' to change column 'kids' according to conditions
UPDATE sales
SET kids = CASE
    WHEN size IN (20, 16, 5, 14, 4, 3, 15, 17, 18, 1, 2, 13, 21) THEN true
    ELSE false
END;
