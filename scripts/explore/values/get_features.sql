with type_feature as (
  select F.field_id,
         LT.type_name,
         LT.type_category,
         LT.observation_type_id,
         U.unit as unit_name,
         U.unit_type_id,
         LV.observation_id,
         LV.acquired,
         LV.quantity,
         LG.group_name
  from field F
  inner join
    (test_demeter.observation LV
     join test_demeter.unit_type U on U.unit_type_id = LV.unit_type_id
     join test_demeter.observation_type LT on LT.observation_type_id = U.observation_type_id
     left join test_demeter.observation_group LG on LG.observation_group_id = LV.observation_group_id
     left join test_demeter.geom LVG on LV.geom_id = LVG.geom_id
    ) on LV.field_id = F.field_id
  where LV.acquired > '2000-01-01' and
          (array_length(%(observation_type_ids)s::bigint[], 1) is null or
          LT.observation_type_id = any(%(observation_type_ids)s::bigint[])) and
          (array_length(%(field_ids)s::bigint[], 1) is null or
          F.field_id = any(%(field_ids)s::bigint[]))

), time_id as (
  select observation_type_id,
         unit_type_id,
         acquired,
         row_number() over (partition by observation_type_id, unit_type_id order by acquired asc) as time_id
  from type_feature
  group by observation_type_id, unit_type_id, acquired
  order by observation_type_id, unit_type_id

), unit_id as (
  select observation_type_id,
         unit_type_id,
         row_number() over () as unit_id
  from time_id
  group by observation_type_id, unit_type_id

), feature_ids as (
  select F.field_id,
         T.observation_type_id,
         T.unit_type_id,
         T.time_id,
         F.observation_id,
         U.unit_id,
         F.quantity,
         row_number() over (partition by F.field_id, T.observation_type_id, U.unit_type_id, F.quantity
                            order by T.acquired asc
                            RANGE BETWEEN INTERVAL '18 hours' PRECEDING AND INTERVAL '18 hours' FOLLOWING
                           ) as dupe_id
  from type_feature F
  join time_id T on T.observation_type_id = F.observation_type_id and
                         T.unit_type_id = F.unit_type_id and
                         T.acquired = F.acquired
  join unit_id U on U.observation_type_id = F.observation_type_id and
                         U.unit_type_id = F.unit_type_id

), feature as (
  select F.field_id,
         F.observation_id,
         F.observation_type_id as feature_observation_type_id,
         F.type_name as feature_observation_type_name,
         F.unit_name,
         I.unit_id as feature_unit_id,
         I.time_id,
         concat_ws('.', F.type_name, F.unit_name, I.time_id) as prefix,
         F.unit_type_id,
         F.quantity,
         F.acquired
  from type_feature F
  join feature_ids I on I.field_id = F.field_id and
                        I.observation_type_id = F.observation_type_id and
                        I.unit_type_id = F.unit_type_id and
                        I.quantity = F.quantity
  where I.dupe_id = 1

) select FT.observation_id,
         (jsonb_build_object(
           'field_id', FT.field_id,
           'quantity', LV.quantity,
           'details', LV.details,
           'acquired', FT.acquired,
           'observation_type_id', FT.feature_observation_type_id,
           'observation_type_name', FT.feature_observation_type_name,
           'unit_type_id', FT.unit_type_id,
           'unit_name', FT.unit_name,
           'internal_ids', jsonb_build_object(
              'time_id', FT.time_id,
              'unit_id', FT.feature_unit_id,
              'column_name', FT.prefix
           )
         )) as column
  from feature FT
  join observation LV on LV.observation_id = FT.observation_id

