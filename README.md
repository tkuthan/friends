# Friends app

A big data homework.

## Task

Input: one or more text files with lines with the following structure:

<name>, <money>, <friend1>, <friend2>, ... , <friendk>

<name> is unique across the whole dataset - imagine e.g. usernames. <money> is a non-negative integer - how much money does the person have. <friend1> ... <friendk> is a set of friends that the given user has. Each user can have a different number of friends. If user A has a friend B, it does not imply that B has a friend A.

Output: username of the user with most wealthy friends. It means the user where the sum of money of his friends is the biggest among all the users. The money of the user himself is not counted, only of his friends.

The size of the input is so big that the code must run on a cluster of machines. Imagine the size of the Facebook user database.

Example:

Input:
```
david, 10, petr, josef, andrea
andrea, 50, josef, martin
petr, 20, david, josef
josef, 5, andrea, petr, david
martin, 100, josef, andrea, david
```

Output:
```
andrea
```
## Build

Friends app is a maven project.

Build the app and run unit tests by:

`mvn clean package`

## Run

### Submit to secure remote cluster

Before submitting to remote yarn cluster, obtain a kerberos TGT and make sure
spark submit can access hadoop configuration (export HADOOP_CONF_DIR if needed).

```
kinit -kt /etc/security/keytabs/hbase.headless.keytab hbase-dr-cluster 
spark-submit --master yarn-cluster --conf spark.hadoop.yarn.timeline-service.enabled=false --class tkuthan.FriendsApp --files friends.csv  friends-0.0.1-SNAPSHOT.jar /user/hbase/friends.csv
```

The output is written to standard output.
In cluster mode, it can be obtained from yarn logs:

`yarn logs -applicationId <appId>`

### Run locally

`spark-submit --class tkuthan.FriendsApp  target/friends-0.0.1-SNAPSHOT.jar data/friends.csv`