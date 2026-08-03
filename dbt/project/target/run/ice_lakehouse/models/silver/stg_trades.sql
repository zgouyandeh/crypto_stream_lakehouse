
    -- back compat for old kwarg name
  
  
  
      
          
          
      
  

  

  merge into silver.stg_trades as DBT_INTERNAL_DEST
      using stg_trades__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.trade_id = DBT_INTERNAL_DEST.trade_id
          

      when matched then update set
         * 

      when not matched then insert *
