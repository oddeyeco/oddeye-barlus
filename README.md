**Odd Eye Barlus**
--------------

**Install**

	git clone https://github.com/oddeyeco/oddeye-barlus.git
	cd oddeye-barlus 
	mvn package 
	cp oddeye-barlus-XXX-SNAPSHOT.war  /opt/jetty/webapps
	cp oddeye-barlus-XXX-SNAPSHOT.war  /opt/tomcat/webapps

**Curl example**

    curl -i -XPOST 'https://api.domain.com/oddeye-barlus/put/tsdb' --data-binary 'UUID=4b795b64-c77b-4e2a-841b-0a8ddddd38e&data={JsonData}'
    
**Json Sample**
 
    { "UUID" : "4b795b64-c77b-4e2a-841b-0a88d61dd38e",
     "tags":{
    	"host":"tag_hostname",
    	"type":"tag_type", 
    	"cluster":"cluster_name", 
    	"group":"host_group",
    	"timestamp" : 1458389652
     },
     "data":{
    	"cpu_user":10,
    	"cpu_idle":11,
    	"cpu_iadle":12,
    	"cpu_iowait":13
    	}
    }


