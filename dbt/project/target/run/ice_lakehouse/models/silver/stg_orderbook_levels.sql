
    -- back compat for old kwarg name
  
  
  
      
          
              
              
          
              
              
          
              
              
          
      
  

  

  merge into silver.stg_orderbook_levels as DBT_INTERNAL_DEST
      using stg_orderbook_levels__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
                  DBT_INTERNAL_SOURCE.product_id = DBT_INTERNAL_DEST.product_id
               and 
                  DBT_INTERNAL_SOURCE.side = DBT_INTERNAL_DEST.side
               and 
                  DBT_INTERNAL_SOURCE.price = DBT_INTERNAL_DEST.price
              

      when matched then update set
         * 

      when not matched then insert *
