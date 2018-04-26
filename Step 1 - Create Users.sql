drop user THINNING cascade;
drop user THINNING_LIVE cascade;

create user THINNING identified by Qwertyu1 default tablespace DATA_064M;
grant connect, resource, create view to THINNING;

create user THINNING_LIVE identified by Qwertyu1 default tablespace DATA_064M;
grant connect, resource, create view to THINNING_LIVE;


begin
  dbms_network_acl_admin.drop_acl      (acl => 'utl_http_THINNING_LIVE.xml');
  dbms_network_acl_admin.create_acl    (acl => 'utl_http_THINNING_LIVE.xml', description => 'HTTP Access', principal   => 'THINNING_LIVE', is_grant => TRUE, privilege   => 'connect');
  dbms_network_acl_admin.add_privilege (acl => 'utl_http_THINNING_LIVE.xml', principal  => 'THINNING_LIVE', is_grant   => TRUE, privilege  => 'resolve'); 
  dbms_network_acl_admin.add_privilege (acl => 'utl_http_THINNING_LIVE.xml', principal  => 'THINNING_LIVE', is_grant   => TRUE, privilege  => 'connect'); 
  dbms_network_acl_admin.assign_acl    (acl => 'utl_http_THINNING_LIVE.xml', host => 'api.bitcoincharts.com', lower_port => 80, upper_port => 80);
  commit;
end;



