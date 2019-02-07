*&---------------------------------------------------------------------*
*& Report z_hana_catalog_tester
*&---------------------------------------------------------------------*
*&
*&---------------------------------------------------------------------*
REPORT z_hana_catalog_tester.

INITIALIZATION.
  DATA lv_string TYPE zcl_hana_catalog=>t_sstring.
  SELECT-OPTIONS s1 FOR lv_string.
  parameters: schema type string OBLIGATORY.

START-OF-SELECTION.

  DATA(lt_views) = new zcl_hana_catalog( )->get_views(
      ir_views     = s1[]
      iv_schema_name = schema ).

  cl_salv_table=>factory(
    IMPORTING
      r_salv_table   = data(lr_table)
    CHANGING
      t_table        = lt_views ).
  lr_table->display( ).
