cd benchmark
javac -cp ../ycsb-0.17.0/lib/core-0.17.0.jar:. Benchmark.java
jar cvf Benchmark.jar Benchmark.class
cd ..
echo "building..."
java -cp ycsb-0.17.0/lib/core-0.17.0.jar:ycsb-0.17.0/lib/HdrHistogram-2.1.4.jar:ycsb-0.17.0/lib/htrace-core4-4.1.0-incubating.jar:ycsb-0.17.0/lib/jackson-core-asl-1.9.4.jar:ycsb-0.17.0/lib/jackson-mapper-asl-1.9.4.jar:benchmark/Benchmark.jar:. site.ycsb.Client -db Benchmark -P ycsb-0.17.0/workloads/workloada
