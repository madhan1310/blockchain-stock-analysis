-- Load the dataset
data = LOAD '/user/training/output.csv' USING PigStorage(',') AS (
    hash:chararray, 
    ver:int, 
    prev_block:chararray, 
    mrkl_root:chararray, 
    time:long, 
    bits:int, 
    fee:long, 
    nonce:long, 
    n_tx:int, 
    size:long, 
    block_index:int, 
    main_chain:boolean, 
    height:int, 
    weight:long
);

-- 1) Count the total number of blocks
total_blocks = FOREACH (GROUP data ALL) GENERATE COUNT(data);

-- Output the result
DUMP total_blocks;

-- 2) Find the largest block height
max_height = FOREACH (ORDER data BY height DESC) GENERATE height;
largest_block_height = LIMIT max_height 1;

-- Output the result
DUMP largest_block_height;

-- 3) Find the largest block height
max_height = FOREACH (ORDER data BY height DESC) GENERATE height;
largest_block_height = LIMIT max_height 1;

-- Filter data for the largest block
largest_block_data = FILTER data BY height == largest_block_height.$0;

-- Extract date and time for the largest block
largest_block_timestamp = FOREACH largest_block_data GENERATE time;

-- Output the result
DUMP largest_block_timestamp;

-- 4) Group data by block height and count transactions
transactions_per_block = FOREACH (GROUP data BY height) GENERATE group AS 
block_height, SUM(data.n_tx) AS total_transactions;

-- Find the block with the highest number of transactions
max_transactions = FOREACH (ORDER transactions_per_block BY total_transactions 
DESC) GENERATE block_height, total_transactions;
highest_transactions_block = LIMIT max_transactions 1;

-- Output the result
DUMP highest_transactions_block;
