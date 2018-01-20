JAR=btreelib.jar

#this is the name of the given project folder
ASSIGNMENT=btree_project_1

#change the ASSIGN path to the path where you have downloaded on your computer
ASSIGN=/home/a/ax/axs8838/Fall\ 2017/Project\ 1

#change the JDKPATH if you are using omega.uta.edu it remains the same
JDKPATH = /opt/jdk1.6.0_20
LIBPATH = $(ASSIGN)/$(ASSIGNMENT)/lib/$(JAR)
CLASSPATH = $(LIBPATH):$(ASSIGN)/$(ASSIGNMENT):$(ASSIGN)/$(ASSIGNMENT)/src
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core
