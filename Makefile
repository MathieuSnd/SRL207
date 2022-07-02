.PHONY: run build clean 

run: build
	java SimpleClient

build:
	javac SimpleClient.java

clean:
	rm -rf *.class