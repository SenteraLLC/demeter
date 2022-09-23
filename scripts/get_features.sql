with recursive ancestry as (
  select parent_field_group_id,
         field_group_id,
         0 as depth
  from test_mlops.field_group
  where field_group_id = %(field_group_id)s
  UNION ALL
  select F.parent_field_group_id,
         F.field_group_id,
         A.depth + 1
  from ancestry A
  join test_mlops.field_group F on F.parent_field_group_id = A.field_group_id

), leaf as (
  select A.parent_field_group_id, A.field_group_id, F.field_id
  from ancestry A
  inner join test_mlops.field F on A.field_group_id = F.field_group_id
  where not exists (select * from test_mlops.field_group F where A.field_group_id = F.parent_field_group_id)

), type_feature as (
  select L.field_id,
         LT.type_name,
         LT.type_category,
         LT.local_type_id,
         U.unit as unit_name,
         U.unit_type_id,
         LV.local_value_id,
         LV.acquired,
         LV.quantity,
         LG.group_name
  from leaf L
  inner join
    (test_mlops.local_value LV
     join test_mlops.unit_type U on U.unit_type_id = LV.unit_type_id
     join test_mlops.local_type LT on LT.local_type_id = U.local_type_id
     left join test_mlops.local_group LG on LG.local_group_id = LV.local_group_id
     left join test_mlops.geom LVG on LV.geom_id = LVG.geom_id
    ) on LV.field_id = L.field_id
  where LV.acquired > '2000-01-01' and
          (array_length(%(local_type_id_whitelist)s::bigint[], 1) is null or
          LT.local_type_id = any(%(local_type_id_whitelist)s::bigint[]))

), time_id as (
  select local_type_id,
         unit_type_id,
         acquired,
         row_number() over (partition by local_type_id, unit_type_id order by acquired asc) as time_id
  from type_feature
  group by local_type_id, unit_type_id, acquired
  order by local_type_id, unit_type_id

), unit_id as (
  select local_type_id,
         unit_type_id,
         row_number() over () as unit_id
  from time_id
  group by local_type_id, unit_type_id

), feature_ids as (
  select F.field_id,
         T.local_type_id,
         T.unit_type_id,
         T.time_id,
         F.local_value_id,
         U.unit_id,
         F.quantity,
         row_number() over (partition by F.field_id, T.local_type_id, U.unit_type_id, F.quantity
                            order by T.acquired asc
                            RANGE BETWEEN INTERVAL '18 hours' PRECEDING AND INTERVAL '18 hours' FOLLOWING
                           ) as dupe_id
  from type_feature F
  join time_id T on T.local_type_id = F.local_type_id and
                         T.unit_type_id = F.unit_type_id and
                         T.acquired = F.acquired
  join unit_id U on U.local_type_id = F.local_type_id and
                         U.unit_type_id = F.unit_type_id

), feature as (
  select F.field_id,
         F.local_value_id,
         F.local_type_id as feature_local_type_id,
         F.type_name as feature_local_type_name,
         F.unit_name,
         I.unit_id as feature_unit_id,
         I.time_id,
         concat_ws('.', F.type_name, F.unit_name, I.time_id) as prefix,
         F.unit_type_id,
         F.quantity,
         F.acquired
  from type_feature F
  join feature_ids I on I.field_id = F.field_id and
                        I.local_type_id = F.local_type_id and
                        I.unit_type_id = F.unit_type_id and
                        I.quantity = F.quantity
  where I.dupe_id = 1

) select FT.local_value_id,
         (jsonb_build_object(
           'field_id', FT.field_id,
           'quantity', LV.quantity,
           'details', LV.details,
           'acquired', FT.acquired,
           'local_type_id', FT.feature_local_type_id,
           'local_type_name', FT.feature_local_type_name,
           'unit_type_id', FT.unit_type_id,
           'unit_name', FT.unit_name,
           'internal_ids', jsonb_build_object(
              'time_id', FT.time_id,
              'unit_id', FT.feature_unit_id,
              'column_name', FT.prefix
           )
         )) as columns
  from feature FT
  join local_value LV on LV.local_value_id = FT.local_value_id

