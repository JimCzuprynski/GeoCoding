-----
-- Script:  Batch_Geocoding.sql
-- Purpose: Demonstrates how to leverage APEX_JSON and APEX WEB_SERVICE to
--          submit a multi-address batch request to an external web service
--          outside of JSON (if needed)
-----

-----
-- Build a new table containing limited geographic data and SDO_GEOMETRY object
-----
DROP TABLE vevo.t_voter_merch_requests PURGE;
CREATE TABLE vevo.t_voter_merch_requests (
    vm_id            NUMBER        NOT NULL
   ,vm_rqst_dt       DATE          NOT NULL
   ,vm_rqstr_name    VARCHAR2(100) NOT NULL
   ,vm_rqstr_addr    VARCHAR2(100) NOT NULL
   ,vm_qty           NUMBER(3,0)   NOT NULL
   ,vm_type          VARCHAR2(20)  NOT NULL
   ,vm_dlvr_id       NUMBER
   ,vm_dlvr_dt       DATE
   ,vm_lat           NUMBER
   ,vm_lng           NUMBER
   ,vm_geopoint      SDO_GEOMETRY
);

-----
-- Create PK IDX
-----
 ALTER TABLE vevo.t_voter_merch_requests
    ADD CONSTRAINT voter_merch_requests_pk 
    PRIMARY KEY (vm_id, vm_rqst_dt)
    USING INDEX (
       CREATE UNIQUE INDEX vevo.voter_merch_requests_pk_idx
           ON vevo.t_voter_merch_requests(vm_id, vm_rqst_dt)
            TABLESPACE vevo_idx
        );

-----
-- Create a corresponding VIEW for the table
-----
CREATE OR REPLACE VIEW vevo.voter_merch_requests AS
  SELECT * FROM vevo.t_voter_merch_requests;

-----
-- Update SDO Geometrics metadata to reflect Longitude and Latitude is being 
-- applied within the table, and which column contains the SDO_GEOMETRY obj
-----
DELETE FROM user_sdo_geom_metadata
 WHERE table_name = 'T_VOTER_MERCH_REQUESTS'
   AND column_name = 'VM_GEOPOINT';

INSERT INTO user_sdo_geom_metadata
VALUES (
    'T_VOTER_MERCH_REQUESTS'
   ,'VM_GEOPOINT'
   ,SDO_DIM_ARRAY(
       SDO_DIM_ELEMENT('Longitude', -180, 180, 0.5)
      ,SDO_DIM_ELEMENT('Latitude',  -90,   90, 0.5)
   )
  ,8307);
  
COMMIT;


-----
-- Now leverage APEX_JSON and APEX_WEB_SERVICE to call a predefined APEX 
-- Web Source Module (WSM) via POSTing JSON output to the GeoCodio API. See
-- the screenshots in this GitHub repository for examples on how to configure
-- the WSM within your APEX application
-----

DECLARE
  -- Processing variables:
  SQLERRNUM     INTEGER := 0;
  SQLERRMSG     VARCHAR2(255);

  -- CLOBs for input and output:
  sent_clob     CLOB;
  recv_clob     CLOB;
  -- JSON parsing variables:
  recv_values   APEX_JSON.T_VALUES;
  mbr_count     PLS_INTEGER;
  vid           VARCHAR2(4000);
  lat           NUMBER(9,6); 
  lng           NUMBER(9,6); 

  -- Process each set of next 40 voters at one time
  CURSOR curNeedLatLng IS
    SELECT 
         TO_CHAR(vm_id) AS van_id
        ,vm_rqstr_addr  AS formatted_address
      FROM vevo.t_voter_merch_requests
     WHERE vm_lat IS NULL 
       AND vm_lng IS NULL
       AND rownum <= 40;

BEGIN
  -----
  -- Create a CLOB containing the necessary address elements
  -----
  APEX_JSON.INITIALIZE_CLOB_OUTPUT;
  APEX_JSON.OPEN_OBJECT;

  FOR i IN curNeedLatLng
    LOOP
      APEX_JSON.WRITE(i.van_id, i.formatted_address);
    END LOOP;

  APEX_JSON.CLOSE_OBJECT;
    
  sent_clob := APEX_JSON.GET_CLOB_OUTPUT;
  APEX_JSON.FREE_OUTPUT;

  DBMS_OUTPUT.PUT_LINE('JSON to be sent: ' || LENGTH(sent_clob));  
  DBMS_OUTPUT.PUT_LINE(sent_clob);  

  -----
  -- Set header values so that incoming input is recognized as JSON
  -----
  APEX_WEB_SERVICE.G_REQUEST_HEADERS.DELETE();
  APEX_WEB_SERVICE.G_REQUEST_HEADERS(1).name := 'Content-Type';
  APEX_WEB_SERVICE.G_REQUEST_HEADERS(1).value := 'application/json';

  recv_clob :=
    APEX_WEB_SERVICE.MAKE_REST_REQUEST(
      p_url =>          'https://api.geocod.io/v1.6/geocode?api_key={YourAPIKeyHere}'
     ,p_http_method =>  'POST'
     ,p_body =>         sent_clob
     );

  -----
  -- Populate the received JSON output into a CLOB, and then process the results
  -----
  APEX_JSON.PARSE(
     p_values => recv_values
    ,p_source => recv_clob
    );

  mbr_count := APEX_JSON.GET_COUNT(p_path => 'results', p_values => recv_values);

  FOR i IN 1 .. mbr_count
    LOOP
      vid := (APEX_JSON.GET_MEMBERS(p_path => 'results', p_values => recv_values)(i));
      lat := APEX_JSON.GET_VARCHAR2(p_path => 'results.'|| vid || '.response.results[1].location.lat', p_values => recv_values);
      lng := APEX_JSON.GET_VARCHAR2(p_path => 'results.'|| vid || '.response.results[1].location.lng', p_values => recv_values);
      DBMS_OUTPUT.PUT_LINE('Voter ID: ' || vid || ' Latitude: ' || lat || ' Longitude: ' || lng);

      UPDATE vevo.t_voter_merch_requests
        SET 
          vm_lat = lat
         ,vm_lng = lng
       WHERE vm_id = TO_NUMBER(vid);
       
    END LOOP;

  COMMIT;

  -----
  -- Finally, apply all updated Lat/Long data in SDO_GEOMETRY type
  -----
  UPDATE vevo.t_voter_merch_requests
     SET vm_geopoint = 
       SDO_GEOMETRY(
           2001
          ,8307
          ,SDO_POINT_TYPE(vm_lng, vm_lat, NULL)
          ,NULL
          ,NULL
       );
  
  COMMIT;

EXCEPTION
  WHEN OTHERS THEN 
    SQLERRNUM := SQLCODE;
    SQLERRMSG := SQLERRM;
    DBMS_OUTPUT.PUT_LINE('Unexpected error: ' || SQLERRNUM || ' - ' || SQLERRMSG);

END;
/
