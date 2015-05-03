drop table if exists practices;
create table practices (
  id varchar(20) primary key,
  name varchar(40),
  polygon text,
  broken integer
);

