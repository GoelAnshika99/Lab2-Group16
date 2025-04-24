{% snapshot snapshot_stock_summary %}

{{
  config(
   target_schema = 'snapshot',
   unique_key = 'sessionID',
   strategy = 'timestamp',
   updated_at = 'ts',
   invalidate_hard_deletes = True
  )
}}

SELECT * FROM {{ ref('stock_summary') }}

{% endsnapshot %}