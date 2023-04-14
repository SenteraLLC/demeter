-- update search path for `postgres`
alter role postgres set search_path = demeter,weather,raster,public;

-- create `demeter_user` with read and write access to all schemas
create user demeter_user with password 'demeter_user_password';
alter role demeter_user set search_path = demeter,weather,raster,public;

-- create `demeter_ro_user` with read only access to all schemas
create user demeter_ro_user with password 'demeter_ro_user_password';
alter role demeter_user set search_path = demeter,weather,raster,public;

-- create `weather_user` with read and write access to weather
create user weather_user with password 'weather_user_password';
alter role weather_user set search_path = weather,public;

-- create `weather_ro_user` with read only access to weather
create user weather_ro_user with password 'weather_ro_user_password';
alter role weather_ro_user set search_path = weather,public;

-- create `raster_user` with read and write access to raster
create user raster_user with password 'raster_user_password';
alter role raster_user set search_path = raster,public;

-- create `raster_ro_user` with read only access to raster
create user raster_ro_user with password 'raster_ro_user_password';
alter role raster_ro_user set search_path = raster,public;