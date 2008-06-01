all:
	javac -cp $(CLASSPATH):..:. TopKRank.java
	javac -cp $(CLASSPATH):..:. TopKRankTest.java

test:
	java -cp $(CLASSPATH):.. graphs.TopKRankTest

clean:
	rm *.class
