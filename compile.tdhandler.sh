javac -cp "jars/*" TDHandler.java -d .
jar cvfm jars/TDHandler.jar MANIFEST.MF *.class
rm TDHandler*.class
