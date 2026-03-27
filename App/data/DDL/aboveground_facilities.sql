-- © 2026 NTT DATA Japan Co., Ltd. & NTT InfraNet All Rights Reserved.

-- 地上附帯施設
-- * RestoreFromTempTable
create table db_fac.data_aboveground_facilities_{provider_id} (
  id bigint not null
  , mg_id character varying(254) not null
  , seq_no character varying(254) not null
  , fac_key character varying(254) not null
  , file_name character varying(254) not null
  , inst_year numeric(4,0)
  , org_update numeric(14,0)
  , xy_prec character varying(8)
  , fac_type character varying(8)
  , qty integer
  , fac_size_h integer not null
  , fac_size_w integer not null
  , fac_size_d integer not null
  , occ_perm date
  , opt_attr text
  , geom geometry(GeometryZ,4326) not null
  , created_by character varying(20) not null
  , created_at timestamp without time zone not null
  , updated_by character varying(20)
  , updated_at timestamp without time zone
  , constraint data_aboveground_facilities_{provider_id}_PKC primary key (id)
) ;

-- インデックス付与
create index data_aboveground_facilities_{provider_id}_geom_idx
  on db_fac.data_aboveground_facilities_{provider_id}
  using GIST (geom);

