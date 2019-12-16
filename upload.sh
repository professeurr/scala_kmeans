

user_id=$1

scp -i ~/.ssh/id_rsa_$user_id -P 993  ./target/scala-2.11/kmeans_scala_klouvi_riva_2.11-1.0.jar $user_id@www.lamsade.dauphine.fr:~/
scp -i ~/.ssh/id_rsa_$user_id -P 993  ./run-scala.sh $user_id@www.lamsade.dauphine.fr:~/

#ssh -i ~/.ssh/id_rsa_user159 -p 993  $user_id@www.lamsade.dauphine.fr # ./run.sh ./kmeans_scala_klouvi_riva_2.11-1.0.jar hdfs:///user/$user_id/iris.data.txt
#exit
