scp -i ~/.ssh/id_rsa_user159 -P 993  ./target/scala-2.11/kmeans_scala_klouvi_riva_2.11-1.0.jar user159@www.lamsade.dauphine.fr:~/
scp -i ~/.ssh/id_rsa_user159 -P 993  run.sh user159@www.lamsade.dauphine.fr:~/

#ssh -i ~/.ssh/id_rsa_user159 -p 993  user159@www.lamsade.dauphine.fr # ./run.sh ./kmeans_scala_klouvi_riva_2.11-1.0.jar hdfs:///user/user159/iris.data.txt
#exit
