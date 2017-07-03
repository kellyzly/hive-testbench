
DROP TABLE IF EXISTS training;
CREATE TABLE training (
ss_store_sk         	bigint, 
ss_sold_time_sk     	bigint,              	                    
ss_item_sk          	bigint,              	                    
ss_customer_sk      	bigint,              	                    
ss_cdemo_sk         	bigint,              	                    
ss_hdemo_sk         	bigint,              	                    
ss_addr_sk          	bigint,              	                    
ss_promo_sk         	bigint,              	                    
ss_ticket_number    	bigint,              	                    
ss_quantity         	int,                 	                    
ss_wholesale_cost   	double 
);
DROP TABLE IF EXISTS testing;
CREATE TABLE testing (
ss_store_sk         	bigint, 
ss_sold_time_sk     	bigint,              	                    
ss_item_sk          	bigint,              	                    
ss_customer_sk      	bigint,              	                    
ss_cdemo_sk         	bigint,              	                    
ss_hdemo_sk         	bigint,              	                    
ss_addr_sk          	bigint,              	                    
ss_promo_sk         	bigint,              	                    
ss_ticket_number    	bigint,              	                    
ss_quantity         	int,                 	                    
ss_wholesale_cost   	double 
);

FROM (
  SELECT
    ss_store_sk,
    ss_sold_time_sk,
    ss_item_sk,
    ss_customer_sk,
    ss_cdemo_sk,
    ss_hdemo_sk,
    ss_addr_sk,
    ss_promo_sk,
    ss_ticket_number,
    ss_quantity,
    ss_wholesale_cost
  FROM store_sales
  order by ss_store_sk
)p
INSERT OVERWRITE TABLE training
  SELECT *
  WHERE pmod(ss_store_sk, 10) IN (1,2,3,4,5,6,7,8,9) -- 90% are training
INSERT OVERWRITE TABLE testing
  SELECT *
  WHERE pmod(ss_store_sk, 10) IN (0) -- 10% are testing
;

