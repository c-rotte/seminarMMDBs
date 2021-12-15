cd benchmark
javac -cp ../ycsb-0.17.0/lib/core-0.17.0.jar:. Benchmark.java
jar cvf Benchmark.jar Benchmark.class
cd ..
echo "building..."
java -cp ycsb-0.17.0/lib/core-0.17.0.jar:benchmark/Benchmark.jar:. site.ycsb.CommandLine -db Benchmark 
