CLASS zcl_hana_catalog DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    INTERFACES if_amdp_marker_hdb.

    TYPES: BEGIN OF ty_views,
             schema_name    TYPE string,
             view_name      TYPE string,
             view_oid       TYPE i,
             comments       TYPE string,
             is_column_view TYPE string,
             view_type      TYPE string,
             create_date    TYPE d,
             create_time    TYPE t,
           END OF ty_views.

    TYPES: tt_views TYPE STANDARD TABLE OF ty_views WITH EMPTY KEY.

    TYPES: BEGIN OF ty_tables,
             schema_name     TYPE string,
             table_name      TYPE string,
             table_oid       TYPE i,
             comments        TYPE string,
             is_column_table TYPE string,
             table_type      TYPE string,
             has_primary_key TYPE string,
             create_date     TYPE d,
             create_time     TYPE t,
           END OF ty_tables.

    TYPES: tt_tables TYPE STANDARD TABLE OF ty_tables WITH EMPTY KEY.

    TYPES: BEGIN OF ty_table_columns,
             schema_name         TYPE string,
             table_name          TYPE string,
             table_oid           TYPE i,
             column_name         TYPE string,
             position            TYPE i,
             data_type_id        TYPE i,
             data_type_name      TYPE string,
             offset              TYPE i,
             length              TYPE i,
             scale               TYPE i,
             is_nullable         TYPE string,
             default_value       TYPE string,
             comments            TYPE string,
             cs_data_type_name   TYPE string,
             ddic_data_type_name TYPE string,
             compression_type    TYPE string,
             index_type          TYPE string,
             is_hidden           TYPE string,
             is_masked           TYPE string,
           END OF ty_table_columns.

    TYPES: tt_table_columns TYPE STANDARD TABLE OF ty_table_columns WITH EMPTY KEY.

    TYPES: BEGIN OF ty_view_columns,
             schema_name         TYPE string,
             view_name           TYPE string,
             view_oid            TYPE i,
             column_name         TYPE string,
             position            TYPE i,
             data_type_id        TYPE i,
             data_type_name      TYPE string,
             offset              TYPE i,
             length              TYPE i,
             scale               TYPE i,
             is_nullable         TYPE string,
             default_value       TYPE string,
             comments            TYPE string,
             cs_data_type_name   TYPE string,
             ddic_data_type_name TYPE string,
             compression_type    TYPE string,
             index_type          TYPE string,
             is_hidden           TYPE string,
             is_masked           TYPE string,
           END OF ty_view_columns.

    TYPES: tt_view_columns TYPE STANDARD TABLE OF ty_view_columns WITH EMPTY KEY.

    TYPES: BEGIN OF ty_schemas,
             schema_name    TYPE string,
             schema_owner   TYPE string,
             has_privileges TYPE string,
             create_date    TYPE d,
             create_time    TYPE t,
           END OF ty_schemas.

    TYPES: tt_schemas TYPE STANDARD TABLE OF ty_schemas WITH EMPTY KEY.

    TYPES: BEGIN OF ty_constraints,
             schema_name     TYPE string,
             table_name      TYPE string,
             column_name     TYPE string,
             position        TYPE i,
             constraint_name TYPE string,
             is_primary_key  TYPE string,
             is_unique_key   TYPE string,
             check_condition TYPE string,
           END OF ty_constraints.

    TYPES: tt_constraints TYPE STANDARD TABLE OF ty_constraints WITH EMPTY KEY.

    TYPES: BEGIN OF ty_session_context,
             host          TYPE string,
             port          TYPE i,
             connection_id TYPE i,
             key           TYPE string,
             value         TYPE string,
             section       TYPE string,
           END OF ty_session_context.

    TYPES: tt_session_context TYPE STANDARD TABLE OF ty_session_context WITH EMPTY KEY.

    TYPES: t_sstring TYPE c LENGTH 256.
    TYPES: tr_string TYPE RANGE OF t_sstring.


    METHODS get_views
      IMPORTING
                ir_views       TYPE tr_string
                iv_schema_name TYPE string
      RETURNING
                VALUE(r_views) TYPE tt_views
      RAISING   cx_shdb_exception.

    METHODS get_tables
      IMPORTING
                ir_tables       TYPE tr_string
                iv_schema_name  TYPE string
      RETURNING
                VALUE(r_tables) TYPE tt_tables
      RAISING   cx_shdb_exception.

    METHODS get_table_columns
      IMPORTING
        iv_table_oid           TYPE i
      RETURNING
        VALUE(r_table_columns) TYPE tt_table_columns.

    METHODS get_view_columns
      IMPORTING
        iv_view_oid           TYPE i
      RETURNING
        VALUE(r_view_columns) TYPE tt_view_columns.

    METHODS get_schemas
      IMPORTING
                ir_schema        TYPE tr_string
      RETURNING
                VALUE(r_schemas) TYPE tt_schemas
      RAISING   cx_shdb_exception.

    METHODS get_constraints
      IMPORTING
                ir_tables            TYPE tr_string
                iv_schema_name       TYPE string
      RETURNING
                VALUE(r_constraints) TYPE tt_constraints
      RAISING   cx_shdb_exception.

    METHODS get_session_context
      RETURNING
        VALUE(r_session_context) TYPE tt_session_context.


  PROTECTED SECTION.

    METHODS get_views_int
      IMPORTING
        VALUE(iv_where) TYPE string
      EXPORTING
        VALUE(r_views)  TYPE tt_views.

    METHODS get_tables_int
      IMPORTING
        VALUE(iv_where) TYPE string
      EXPORTING
        VALUE(r_tables) TYPE tt_tables.

    METHODS get_table_columns_int
      IMPORTING
        VALUE(iv_table_oid)    TYPE i
      EXPORTING
        VALUE(r_table_columns) TYPE tt_table_columns.

    METHODS get_view_columns_int
      IMPORTING
        VALUE(iv_view_oid)    TYPE i
      EXPORTING
        VALUE(r_view_columns) TYPE tt_view_columns.

    METHODS get_schemas_int
      IMPORTING
        VALUE(iv_where)  TYPE string
      EXPORTING
        VALUE(r_schemas) TYPE tt_schemas.

    METHODS get_constraints_int
      IMPORTING
        VALUE(iv_where)      TYPE string
      EXPORTING
        VALUE(r_constraints) TYPE tt_constraints.

    METHODS get_session_context_int
      EXPORTING
        VALUE(r_session_context) TYPE tt_session_context.

  PRIVATE SECTION.

ENDCLASS.



CLASS zcl_hana_catalog IMPLEMENTATION.


  METHOD get_views_int BY DATABASE PROCEDURE FOR HDB LANGUAGE SQLSCRIPT.

    EXEC 'SELECT SCHEMA_NAME, VIEW_NAME, VIEW_OID, ' ||
                'COMMENTS, ' ||
                'IS_COLUMN_VIEW, VIEW_TYPE, ' ||
                'to_dats(CREATE_TIME) as CREATE_DATE, ' ||
                'to_nvarchar(to_time(CREATE_TIME), ''HHMMSS'') as CREATE_TIME ' ||
                'FROM SYS.VIEWS WHERE '||
           iv_where
           into r_views;

  ENDMETHOD.

  METHOD get_views.
    DATA(lv_where) = cl_shdb_seltab=>combine_seltabs(
       it_named_seltabs = VALUE #(
         ( name = 'VIEW_NAME' dref = REF #( ir_views[] )  ) ) ).

    IF lv_where IS INITIAL.
      lv_where = | SCHEMA_NAME = '{ iv_schema_name }' | .
    ELSE.
      lv_where = | { lv_where } AND SCHEMA_NAME = '{ iv_schema_name }' | .
    ENDIF.

    me->get_views_int(
      EXPORTING
        iv_where = lv_where
      IMPORTING
        r_views  = r_views ).

  ENDMETHOD.

  METHOD get_tables_int BY DATABASE PROCEDURE FOR HDB LANGUAGE SQLSCRIPT.

    EXEC 'SELECT SCHEMA_NAME, TABLE_NAME, TABLE_OID, ' ||
                'COMMENTS, ' ||
                'IS_COLUMN_TABLE, TABLE_TYPE, HAS_PRIMARY_KEY, ' ||
                'to_dats(CREATE_TIME) as CREATE_DATE, ' ||
                'to_nvarchar(to_time(CREATE_TIME), ''HHMMSS'') as CREATE_TIME ' ||
                'FROM SYS.TABLES WHERE '||
           iv_where
           into r_tables;

  ENDMETHOD.

  METHOD get_tables.
    DATA(lv_where) = cl_shdb_seltab=>combine_seltabs(
       it_named_seltabs = VALUE #(
         ( name = 'TABLE_NAME' dref = REF #( ir_tables[] )  ) ) ).

    IF lv_where IS INITIAL.
      lv_where = | SCHEMA_NAME = '{ iv_schema_name }' | .
    ELSE.
      lv_where = | { lv_where } AND SCHEMA_NAME = '{ iv_schema_name }' | .
    ENDIF.

    me->get_tables_int(
      EXPORTING
        iv_where = lv_where
      IMPORTING
        r_tables  = r_tables ).

  ENDMETHOD.

  METHOD get_table_columns_int BY DATABASE PROCEDURE FOR HDB LANGUAGE SQLSCRIPT OPTIONS READ-ONLY.

    R_TABLE_COLUMNS =
         SELECT SCHEMA_NAME, TABLE_NAME, TABLE_OID, COLUMN_NAME,
                POSITION, DATA_TYPE_ID, DATA_TYPE_NAME, OFFSET,
                LENGTH, SCALE, IS_NULLABLE, DEFAULT_VALUE,
                COMMENTS, CS_DATA_TYPE_NAME, DDIC_DATA_TYPE_NAME,
                COMPRESSION_TYPE, INDEX_TYPE, IS_HIDDEN, IS_MASKED
                FROM SYS.TABLE_COLUMNS
               WHERE TABLE_OID = :iv_table_oid
               ORDER BY POSITION;
  ENDMETHOD.

  METHOD get_table_columns.
    me->get_table_columns_int(
     EXPORTING
       iv_table_oid = iv_table_oid
     IMPORTING
       r_table_columns  = r_table_columns ).

  ENDMETHOD.

  METHOD get_view_columns_int BY DATABASE PROCEDURE FOR HDB LANGUAGE SQLSCRIPT OPTIONS READ-ONLY.

    R_VIEW_COLUMNS =
         SELECT SCHEMA_NAME, VIEW_NAME, VIEW_OID, COLUMN_NAME,
                POSITION, DATA_TYPE_ID, DATA_TYPE_NAME, OFFSET,
                LENGTH, SCALE, IS_NULLABLE, DEFAULT_VALUE,
                COMMENTS, CS_DATA_TYPE_NAME, DDIC_DATA_TYPE_NAME,
                COMPRESSION_TYPE, INDEX_TYPE, IS_HIDDEN, IS_MASKED
                FROM SYS.VIEW_COLUMNS
               WHERE VIEW_OID = :iv_view_oid
               ORDER BY POSITION;
  ENDMETHOD.

  METHOD get_view_columns.
    me->get_view_columns_int(
     EXPORTING
       iv_view_oid = iv_view_oid
     IMPORTING
       r_view_columns  = r_view_columns ).

  ENDMETHOD.

  METHOD get_schemas_int BY DATABASE PROCEDURE FOR HDB LANGUAGE SQLSCRIPT.

    EXEC 'SELECT SCHEMA_NAME, SCHEMA_OWNER, HAS_PRIVILEGES, ' ||
                'to_dats(CREATE_TIME) as CREATE_DATE, ' ||
                'to_nvarchar(to_time(CREATE_TIME), ''HHMMSS'') as CREATE_TIME ' ||
                'FROM SYS.SCHEMAS WHERE '||
           iv_where
           into r_schemas;

  ENDMETHOD.

  METHOD get_schemas.
    DATA(lv_where) = cl_shdb_seltab=>combine_seltabs(
       it_named_seltabs = VALUE #(
         ( name = 'SCHEMA_NAME' dref = REF #( ir_schema[] )  ) ) ).

    me->get_schemas_int(
    EXPORTING
      iv_where = lv_where
    IMPORTING
      r_schemas  = r_schemas ).

  ENDMETHOD.

  METHOD get_constraints_int BY DATABASE PROCEDURE FOR HDB LANGUAGE SQLSCRIPT.

    EXEC 'SELECT SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, ' ||
                'POSITION, CONSTRAINT_NAME, ' ||
                'IS_PRIMARY_KEY, IS_UNIQUE_KEY, CHECK_CONDITION  ' ||
                'FROM SYS.CONSTRAINTS WHERE '||
           iv_where
           into r_constraints;

  ENDMETHOD.

  METHOD get_constraints.
    DATA(lv_where) = cl_shdb_seltab=>combine_seltabs(
       it_named_seltabs = VALUE #(
         ( name = 'TABLE_NAME' dref = REF #( ir_tables[] )  ) ) ).

    IF lv_where IS INITIAL.
      lv_where = | SCHEMA_NAME = '{ iv_schema_name }' | .
    ELSE.
      lv_where = | { lv_where } AND SCHEMA_NAME = '{ iv_schema_name }' | .
    ENDIF.

    me->get_constraints_int(
      EXPORTING
        iv_where = lv_where
      IMPORTING
        r_constraints  = r_constraints ).

  ENDMETHOD.

  METHOD get_session_context_int BY DATABASE PROCEDURE FOR HDB LANGUAGE SQLSCRIPT OPTIONS READ-ONLY.

    R_SESSION_CONTEXT =
         SELECT HOST, PORT, CONNECTION_ID, KEY, VALUE, SECTION
                FROM SYS.M_SESSION_CONTEXT
               WHERE CONNECTION_ID = CURRENT_CONNECTION;

  ENDMETHOD.

  METHOD get_session_context.
    me->get_session_context_int(
     IMPORTING
       r_session_context  = r_session_context ).

  ENDMETHOD.
ENDCLASS.
