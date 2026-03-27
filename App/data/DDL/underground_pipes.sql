-- © 2026 NTT DATA Japan Co., Ltd. & NTT InfraNet All Rights Reserved.

-- 埋設管路
-- * RestoreFromTempTable
create table db_fac.data_underground_pipes_{provider_id} (
  id bigint not null
  , mg_id character varying(254) not null
  , seq_no character varying(254) not null
  , fac_key character varying(254) not null
  , file_name character varying(254) not null
  , inst_year numeric(4,0)
  , org_update numeric(14,0)
  , xy_prec character varying(8)
  , fac_type character varying(8)
  , out_diam integer not null
  , out_d_prec character varying(8)
  , in_diam integer
  , thickness integer
  , pipe_mat character varying(8)
  , str_depth integer not null
  , end_depth integer not null
  , depth_prec character varying(8)
  , ex_length integer
  , row_num integer not null
  , column_num integer not null
  , occ_perm date
  , ret_type character varying(8)
  , opt_attr text
  , clearance integer not null
  , pipe_type character varying(254) not null
  , geom geometry(GeometryZ,4326) not null
  , created_by character varying(20) not null
  , created_at timestamp without time zone not null
  , updated_by character varying(20)
  , updated_at timestamp without time zone
  , constraint data_underground_pipes_{provider_id}_PKC primary key (id)
) ;

-- インデックス付与
create index data_underground_pipes_{provider_id}_geom_idx
  on db_fac.data_underground_pipes_{provider_id}
  using GIST (geom);

