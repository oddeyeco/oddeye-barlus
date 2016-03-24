**Odd Eye Coconut**
--------------
War @ update chi anum

Apache Ant karas buils anes

im mot senc 
ant -f /home/vahan/NetBeansProjects/oddeye/oddeye-barlus -Dbrowser.context=/home/vahan/NetBeansProjects/oddeye/oddeye-barlus -DforceRedeploy=false -Dnb.internal.action.name=rebuild clean dist

asxatuma
inc tvuma vor /home/vahan/NetBeansProjects/oddeye/oddeye-barlus poxes qu path-i pti vor ashxati




![Odd Eye](https://netangels.net/utils/odd_eye.jpg)

**Curl example**

    curl -i -XPOST 'https://barlus.oddeye.co/coconuts/write' --data-binary 'UUID=4b795b64-c77b-4e2a-841b-0a88d61dd38e&data={JsonData}'
    
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


