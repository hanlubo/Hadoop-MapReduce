#! /bin/bash

javac -cp hadoop-core-1.2.1.jar -d wordcount/ WordCount.java
jar cvf wordcount.jar -C wordcount/ .
