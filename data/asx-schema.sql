-- Address contains the registered address of the company (excluding the zip code and country
-- Zip is the zip code of the Address
-- Country is the incorporation country of the company (same as the country for the Address)
CREATE TABLE Company (
  Code char(3) primary key,
  Name text not null,
  Address text default null,
  Zip varchar(10) default null,
  Country varchar(40) default null
);

-- Person may contain person name, title and/or qualification
CREATE TABLE Executive (
  Code char(3) references Company(Code),
  Person varchar(200) not null ,
  primary key (Code, Person)
);

CREATE TABLE Category (
  Code char(3) primary key references Company(Code),
  Sector varchar(40) default null,
  Industry varchar(80) default null
);

CREATE TABLE ASX (
  data varchar (100),
  Code char(3) references Company(Code),
  Volume integer not null,
  Price numeric not null,
  primary key (data, Code)
);

CREATE TABLE Rating (
  Code char(3) references Company(Code),
  Star integer default 3 check (Star > 0 and Star < 6)
);