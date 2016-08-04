# Hypernym detection with Map-Reduce and machine learning

This app was written as part of a Distributed Systems Programming course taken in BGU. It is an implementation of [Learning syntactic patterns for automatic hypernym discovery](http://ai.stanford.edu/~rion/papers/hypernym_nips05.pdf) by R. Snow, D. Jurafsky and A. Ng.

It uses an Amazon EMR cluster to process huge amounts of data of [Google Syntactic Ngrams]
(http://storage.googleapis.com/books/syntactic-ngrams/index.html). We processed and trained our classifier on the set `English All > Biarcs 00`.

The ngrams are parsed, stemmed and then a dependency tree is constructed. We use this tree to extract shortest paths between nouns. We use these shortest paths to emit a file, which contains truth data from a pre-tagged test set. This file is then tun through [WEKA](http://www.cs.waikato.ac.nz/ml/weka/) to train classifiers.
The test set is stored in an S3 bucket.

For further information, please consult the [assignment description](https://www.cs.bgu.ac.il/~dsp162/Assignments/Assignment_3).

## System configuration

1. Hadoop 2.7.2
2. Java 1.7.79
3. OS X 10.10.5 Yosemite
4. Required definitions in `~/.bash_profile`:
```
    export JAVA_HOME=$(/usr/libexec/java_home)
    export HADOOP_HOME=</path/to/hadoop>
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
    export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
    export HADOOP_OPTS=-Djava.library.path=$HADOOP_HOME/lib/native
    export PATH=$JAVA_HOME/bin:$PATH
    export PATH=$HADOOP_HOME/bin:$PATH
    export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
    export HADOOP_USERNAME=<your username>
```

## How to run this on your computer

* Setup your bucket name in `Phase2.java`.
* Setup all the JobFlowRequest parameters (buckets, etc.) in `HDetector.java` to your liking. Don't forget to set the EC2 keyname to one that you actually have pre-configured.
* Setup your `CLASSPATH` environment variable to point to Hadoop and AWS SDK for Java.
* Compile and pack the app to into a JAR. If you're required to use a manifest file, DON'T specify a main class, as this overrides the definitions for the EMR JobFlow request.
* Upload this JAR to your S3 bucket.
* To run the app on EMR, while in the folder with all the `.class` files:
```
    java HDetector <k>
```
where `<k>` is the `DPmin` value. 5 is a good starting point for the aforementioned input corpus.

* When the Hadoop cluster finishes execution, its output resides in your S3 bucket. Download it and post-process it with `PostProcessor.java` - it's a simple and self-explanatory app, just don't forget to set the bucket name properly.
* *NOTE* there's an unresolved bug when running the app locally (debug mode) with Java 1.7 and OS X. The only way around it is to compile and run with Java 1.8, and then when all's good, switch back to Java 1.7 in order to compile and create the JAR (EMR doesn't like Java 1.8 for some odd reason).

## How to run this in debug mode
* In IntelliJ, edit your run configuration like so:

Main class: `org.apache.hadoop.util.RunJar`

Program arguments: `out/artifacts/HDetector/HDetector.jar HDetector 5`

# License

The MIT License (MIT)

Copyright (c) 2016 Asaf Chelouche, Ben Ben-Chaya

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
