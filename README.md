## main java 파일
- jobs/EventLoader.java

## 테스트 환경
- hadoop 2.7.3
- spark 2.3.0
- hive 2.3.4

## 테스트 방법
- 입력 csv 파일의 용량이 크기 때문에 상위 10줄을 가지고 새로운 csv 파일로 만들어서 도커 내 hdfs에 올라가 있는 상태입니다. 
- 4번 초기화 단계를 마치면, http://localhost:50070/explorer.html#/user/hive/warehouse/data 를 통해 확인할 수 있습니다.


**1. 도커 이미지 다운로드**
> docker pull kelle111/hadoop2_spark_hive:1.0.0

**2. 도커 컨테이너 생성및 실행**
> docker run --privileged -d --hostname namenode --name [컨테이너 이름]  -p 50070:50070 -p 50090:50090 -p 4040:4040 -p 8080:8080 -p 8088:8088 -p 22:22 kelle111/hadoop2_spark_hive
* 예시
> docker run --privileged -d --hostname namenode --name test -p 50070:50070 -p 50090:50090 -p 4040:4040 -p 8080:8080 -p 8088:8088 -p 22:22 kelle111/hadoop2_spark_hive

**3. 도커 컨테이너 진입**
> docker exec -it test

**4. 초기화**
```bash
cd
source ./.bashrc
source /etc/profile
start-all.sh
/root/spark/sbin/start-master.sh
cd /root/hive/bin
hive --service hiveserver2
hive --service metastore
```
**5. 초기화 결과 확인**
```bash
[root@namenode bin]# jps
1127 Master
599 SecondaryNameNode
425 DataNode
1835 Jps
301 NameNode
765 ResourceManager
1039 NodeManager
1711 RunJar
1407 RunJar
```
- jps 가 위와 같이 나왔으면 정상적으로 하둡 환경 세팅이 완료됐음을 의미합니다.
- 만약 DataNode 등이 없는 경우 stop-all.sh 후, 다시 start-all.sh 를 실행합니다.
- Master 가 없는 경우 spark 서버를 찾아 kill 하고 start-master.sh 를 다시 실행합니다.
- RunJar 2 개중 하나가 없는 경우 `ps -ef | grep hive` 를 이용해서 hiveserver2 랑 hivemetastore 2개 중 올라오지 않은것을 다시 올립니다.

**6. Spark Application 실행**
```bash
cd /root/jars
./sparkscript.sh (3~5분 정도 소요됨. 컴퓨터 성능에 따라 더 오래걸릴 수 있음)
```

**7. 실행결과 확인**
```bash
cd /root/hive/bin
hive #하이브 진입 
>> show tables;
>> select * from event;
```

## Hive Metastore 확인
> mysql u -hive -p
12341234

