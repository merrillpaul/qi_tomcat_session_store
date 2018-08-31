# Tomcat Session Store for QI and removing Stick Session
Tomcat's default session store is maintained in each server's memory. This eventually causes a dependency of configuring the loadbalancer ( may it be regualr Apache/Nginx or AWS ELB/ALB's ).
The issue with that is the load balancer is not fully utilized and already connected users always get connected to the previous server, hence the stickiness. The upstream tomcat servers do not participate in distributing the load equally.

The solution is to configure a shared memory resource, which is scalable, and which serves a data store for user sessions created by tomcat. The shared memory store that is prototyped is AWS Elasticache with REDIS engine on non clustered mode with sensible replica handling. This needs to be setup by AIDevops.

## Setup
### Shared Libraries
Copy the shared libraries from here [shared libs files](https://drive.google.com/open?id=1Vo-C_hgmsBEMwv7iYEKli5yPFrrSD8Oe) to <tomcat>/lib.

### Context.xml entries
We need to update <tomcat>/conf/context.xml to enable the shared session store.
```
... etc
<Context ...
... etc

<Manager className="org.apache.catalina.session.PersistentManager"
        distributable="true"
        saveOnRestart="true"
        maxActiveSessions="-1"
        minIdleSwap="10" maxIdleSwap="30" maxIdleBackup="10">
        <Store  className="com.pearson.tomcat.session.RedisPooledStore"
                redisHost="qi-dev-ecache.nkim6x.0001.cac1.cache.amazonaws.com"
                redisPort="6379"
                keyName="qi-sessions"
                maxTotal="500"
                maxIdle="128"
                minIdle="30"
        />
    </Manager>
</Context>
```

The ***redisHost*** is the endpoint for the ***AWS Elasticache cluster***. The max.. are for the connection pool configuration to get a redis connection. AIDevops will setup the redis cluster and ensure that the EC2 instances have access to it. AIDevops would apply the diligence on the resources of the cluster, for eg whether DEV and QA can be small to moderate instance types and higher environments have larger capacities.

More information on Tomcat's session manager is available [here](https://tomcat.apache.org/tomcat-8.0-doc/config/manager.html)

### Remove Session Stickiness configs
We need to remove any session stickiness configuration in the AWS ALB/ELB and make the session id that is generated without any indication of the upstream ec2 instance. For eg, right now the session id generated for Qi Central is in this format :- `088D8AAD96140716E68A69E62AF5CA17.qinteractive-187`. Once the stickiness configuration is removed from the ALB/ELB , we also need to remove the injection of the suffix (`.qinteractive-187`)

## Testing
### Local Laptop
- Install redis-cli and redis-server from [here](https://redis.io/download) or using `homebrew`
- Make sure redis-server and redis-cli are in the path
- Add the Context.xml entries and the shared lib jar files as mentioned above
- Deploy a choose-share.war into your local tomcat. Make sure you have setup jndi datasource.
- Start tomcat and try to create an assessment in the wizard , but do not complete the creation. At last page step back ( using the back button ) and remain on the page
- Kill and restart your local tomcat
- Test whether your can continue the assessment creation from where you left off.
- Note there are testcases in the code base which tests all redis commands locally

### AWS Dev
- Follow all the steps above except for installing redis

### AWS QA
QA and higher environments are setup in such a way that there are multiple ec2 ( tomcat instances) behind  AWS ALB. Hence to test this , we need to ensure that first all sticky session configurations are removed and that the load balancing has got sensible configurations.
- Shutdown all tomcats
- Add the context.xml entries and the jar files into the shared lib as mentioned above
- Restart all tomcats
- Go to Central and try to create an assessment in the wizard , but do not complete the creation. At last page step back ( using the back button ) and remain on the page.
- Notice the logs and ensure which tomcat the user was connected to .
- Shutdown that tomcat instance.
- Try to complete the creation of an assessment from where you left off. You should be able to successfully create an assessment
