select json_agg(K.*) as execution_keys,
       json_agg(O.*) as observation_arguments,
       json_agg(H.*) as http_arguments,
       json_agg(SI.*) as s3_input_arguments,
       json_agg(SO.*) as s3_output_arguments
from execution E
join execution_key K on E.execution_id = K.execution_id
join observation_argument O on E.execution_id = O.execution_id
join http_argument H on E.execution_id = H.execution_id
join s3_input_argument SI on E.execution_id = SI.execution_id
join s3_output_argument SO on E.execution_id = SO.execution_id
where E.function_id = %(function_id)s
group by E.execution_id

