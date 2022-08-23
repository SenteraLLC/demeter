with latest_function as (
  select *
  from function F
  where F.function_name = %(function_name)s and F.major = %(major)s and F.function_type_id = %(function_type_id)s
  order by minor desc
  limit 1

), local_inputs as (
  select F.function_id,
         jsonb_agg(
           jsonb_build_object('type_name', LT.type_name,
                              'type_category', LT.type_category
                             )
         ) as local_types
  from latest_function F
  join local_parameter LP on F.function_id = LP.function_id
  join local_type LT on LP.local_type_id = LT.local_type_id
  group by F.function_id

), keyword_inputs as (
  select F.function_id,
         jsonb_agg(
           jsonb_build_object('keyword_name', K.keyword_name,
                              'keyword_type', K.keyword_type
                             )
         ) as keyword_types
  from latest_function F
  join keyword_parameter K on K.function_id = F.function_id
  group by F.function_id

), s3_inputs as (
  select F.function_id,
         jsonb_agg(
           jsonb_build_object('type_name', S3T.type_name)
         ) as s3_types,
         jsonb_agg(
           jsonb_build_object('driver', S3TD.driver,
                              'has_geometry', S3TD.has_geometry
                             )
          ) as s3_dataframe_types
  from latest_function F
  join s3_input_parameter S3I on F.function_id = S3I.function_id
  join s3_type S3T on S3I.s3_type_id = S3T.s3_type_id
  left join s3_type_dataframe S3TD on S3T.s3_type_id = S3TD.s3_type_id
  group by F.function_id

), http_inputs as (
  select F.function_id,
         jsonb_agg(
           jsonb_build_object('type_name', HT.type_name,
                              'verb', HT.verb,
                              'uri', HT.uri,
                              'uri_parameters', HT.uri_parameters,
                              'request_body_schema', HT.request_body_schema
                             )
         ) as http_types
  from latest_function F
  join http_parameter HP on F.function_id = HP.function_id
  join http_type HT on HP.http_type_id = HT.http_type_id
  group by F.function_id

), s3_outputs as (
  select F.function_id,
         jsonb_agg(
           jsonb_build_object('type_name', S3T.type_name)
         ) as s3_types,
         jsonb_agg(
           jsonb_build_object('driver', S3TD.driver,
                              'has_geometry', S3TD.has_geometry
                             )
          ) as s3_dataframe_types
  from latest_function F
  join s3_output_parameter S3O on F.function_id = S3O.function_id
  join s3_type S3T on S3O.s3_type_id = S3T.s3_type_id
  left join s3_type_dataframe S3TD on S3T.s3_type_id = S3TD.s3_type_id
  group by F.function_id

) select F.function_id, F.function_name, F.major, F.minor,
         coalesce(LI.local_types, '[]'::jsonb) as local_inputs,
         coalesce(K.keyword_types, '[]'::jsonb) as keyword_inputs,
         coalesce(S3I.s3_types, '[]'::jsonb) as s3_inputs,
         coalesce(S3I.s3_dataframe_types, '[]'::jsonb) as s3_dataframe_inputs,
         coalesce(HI.http_types, '[]'::jsonb) as http_inputs,
         coalesce(S3O.s3_types, '[]'::jsonb) as s3_outputs,
         coalesce(S3I.s3_dataframe_types, '[]'::jsonb) as s3_dataframe_outputs
  from latest_function F
  left join local_inputs LI on F.function_id = LI.function_id
  left join keyword_inputs K on F.function_id = K.function_id
  left join http_inputs HI on F.function_id = HI.function_id
  left join s3_inputs S3I on F.function_id = S3I.function_id
  left join s3_outputs S3O on F.function_id = S3O.function_id
