with field_operation_maybe_dupe as (
  select field_id,
         name,
         performed,
         row_number() over (partition by field_id, name
                            order by performed asc
                            RANGE BETWEEN INTERVAL '6 hours' PRECEDING AND INTERVAL '6 hours' FOLLOWING
                           ) as dupe_id
  from test_demeter.operation
  where performed > '2000-01-01' and
        field_id = any(%(field_ids)s::bigint[])

), field_operations as (
   select field_id,
          name,
          jsonb_agg(performed) as performed
  from field_operations_maybe_dupe
  where dupe_id = 1
  group by field_id, name

), planting_maybe_dupe as (
  select P.field_id,
         C.species,
         C.cultivar,
         P.performed,
         P.details as planting_details,
         row_number() over (partition by P.field_id
                            order by P.performed asc
                            RANGE BETWEEN INTERVAL '6 hours' PRECEDING AND INTERVAL '6 hours' FOLLOWING
                           ) as dupe_id
  from test_demeter.planting P
  join test_demeter.crop_type C on P.crop_type_id = C.crop_type_id
  where P.field_id = any(%(field_ids)s::bigint[]) and P.performed > '2000-01-01'

), planting as (
  select field_id,
         species,
         cultivar,
         jsonb_agg(performed) as performed
  from planting_maybe_dupe
  where dupe_id = 1
  group by field_id, species, cultivar

) select F.field_id,
         jsonb_build_object(
           'operations', X.operations,
           'plantings', Y.plantings,
           'field_name', F.name,
           'field_external_id', F.external_id,
           'sentera_id', F.details->'AlmanacSenteraId',
           'farm_name', FARM.name,
           'region_name', REGION.name,
           'country_name', COUNTRY.name
         ) as field_meta
  from field F
  join grouper FARM on F.grouper_id = FARM.grouper_id
  join grouper REGION on FARM.parent_grouper_id = REGION.grouper_id
  join grouper COUNTRY on REGION.parent_grouper_id = COUNTRY.grouper_id
  left join (
    select field_id,
           jsonb_agg(
             jsonb_build_object(
               'name', name,
               'performed', performed
             )
           ) as operations
    from field_operations
    where name is not null and performed is not null
    group by field_id
  ) X on F.field_id = X.field_id
  left join (
    select field_id,
           jsonb_agg(
             jsonb_build_object(
               'species', species,
               'cultivar', cultivar,
               'performed', performed
             )
           ) as plantings
    from planting
    where species is not null and performed is not null
    group by field_id
  ) Y on X.field_id = Y.field_id
