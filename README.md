![Odd Eye](https://netangels.net/utils/odd_eye.jpg)


**Curl example**

    curl -i -XPOST 'https://barlus.oddeye.co/coconuts/write' --data-binary 'UUID=3b795b64-c77b-4e2a-841b-0a88d61dd38e&data={JsonData}'
    
**Json Sample**
 
    { "UUID" : "3b795b64-c77b-4e2a-841b-0a88d61dd38e",
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


