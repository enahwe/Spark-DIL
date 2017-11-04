# Spark-DIL
A Spark Library for Data Integration

### A simple API to ingest data
Spark DIL is a set of around a dozen of useful import/export Spark functions (developed in Python for the moment), helping you to directly ingest any kind of external data within your Big Data platform.

If you don't want to waste your time for integrating connectors such as Flume, Kafka and others, then Spark DIL can be your solution.

### Ingest all kind of data
The external data source types can be both text files, binary files but also data in your local memory.

For example, using the local memory allows you to extend the possibilities of data integration by using other standard APIs for other sources like FTP, FTPS, SFTP, and so on.

### Jupyter compliant
Obviously, Spark DIL can be used into your Jupyter projects and also into other kinds of Notebooks.

### An original way to ingest the binary data
To store binary files, the trick is that they are willingly transformed into encoded Base 64 text format, this in order to split more easily the binary data whilst keeping a good and homogeneous balance at the level of the data storage through all the nodes of your Big Data cluster.

Moreover, binary data can be compressed even before to be encoded into Base 64 text format, this allows you to keep in the majority of cases a good enough compression ratio (even though the ratio is systematically reduced by 33% because of the Base 64 encoding).

## Import functions (from Spark Driver to Cluster)
Import a local text file to a RDD:
```
<RDD> importFromLocalTextFileToRDD(sparkContext, localTextFilePath, splitChar)
```

Import a local text file to a cluster text file:
```
<void> importFromLocalTextFileToClusterTextFile(sparkContext, localTextFilePath, clusterTextFilePath, splitChar)

```

Import a local binary file to a RDD encoded into a Base 64 text format:
```
<RDD> importFromLocalBinaryFileToBase64RDD(sparkContext, localBinaryFilePath, splitData=True)
```

Import a local binary file to a cluster binary file encoded into Base 64 text format:
```
<void> importFromLocalBinaryFileToClusterBase64File(sparkContext, localBinaryFilePath, clusterBase64FilePath, splitData=True)
```

Import bytes from local memory to a RDD:
```
<RDD> importFromLocalMemoryToRDD(sparkContext, dataBytes, splitChar)
```

Import bytes from local memory to a cluster text file:
```
<void> importFromLocalMemoryToClusterTextFile(sparkContext, dataBytes, clusterTextFilePath, splitChar)
```

Import bytes from local memory to a RDD encoded into Base 64 text format:
```
<RDD> importFromLocalMemoryToBase64RDD(sparkContext, dataBytes, splitData=True)
```

Import bytes from local memory to a cluster file encoded into Base 64 text format:
```
<void> importFromLocalMemoryToClusterBase64File(sparkContext, dataBytes, clusterBase64FilePath, splitData=True)
```

## Export functions (from Cluster to Spark Driver)
Export from a RDD to local memory:
```
<Bytes> exportFromRDDToLocalMemory(rdd, splitChar)
```

Export from a RDD to a local text file:
```
<void> exportFromRDDToLocalTextFile(rdd, localTextFilePath, splitChar)
```

Export from a cluster text file to a local text file:
```
<void> exportFromClusterTextFileToLocalTextFile(sparkContext, clusterTextFilePath, localTextFilePath, splitChar)
```

Export from a cluster text file to local memory:
```
<Bytes> exportFromClusterTextFileToLocalMemory(sparkContext, clusterTextFilePath, splitChar)
```

Export from a cluster binary file (encoded in Base 64 text format) to a local binary file:
```
<void> exportFromClusterBase64FileToLocalBinaryFile(sparkContext, clusterBase64FilePath, localBinaryFilePath)
```

Export from a cluster binary file (encoded in Base 64 text format) to local memory:
```
<Bytes> exportFromClusterBase64FileToLocalMemory(sparkContext, clusterBase64FilePath)
```

## Other useful functions
```
<Bytes> loadBytesFromLocalFile(localFilePath)

<void> saveBytesToLocalFile(dataBytes, localFilePath)

<void> zipSingleLocalFile(localFilePath, localOutputCompressedFileDir, suffixeDir=False)

<Bytes> zipLocalMemory(itemName, dataBytes)

<OutputFilePath> unzipFirstItemFromLocalFile(localCompressedFilePath, localOutputFileDir)

<Bytes> unzipFirstItemFromLocalMemory(compressedDataBytes)

<void> deleteTmpLocalFile(tmpLocalFilePath, delParentDirIfEmpty=False)

<void> deleteLocalFile(localFilePath)

<Bytes> encodeBytesToBase64Text(dataBytes, splitBase64Text=True)

<Bytes> decodeBase64TextToBytes(base64Text)
```

## Import examples
```
# Example 01  - Import a local text file to a RDD, then save the RDD into the cluster:
textRDD = importFromLocalTextFileToRDD(sc, "./data/t8-shakespeare.txt", '\n')
textRDD.saveAsTextFile("/user/spark/spark-dil-example-01/t8-shakespeare")

# Example 02  - Import a local text file to a text file into the cluster:
importFromLocalTextFileToClusterTextFile(sc, "./data/t8-shakespeare.txt", "/user/spark/spark-dil-example-02/t8-shakespeare", '\n')

# Example 03a - Import a local binary file to a RDD encoded into one part (one block on disk) of Base 64 text format, then save the RDD into the cluster:
base64RDD = importFromLocalBinaryFileToBase64RDD(sc, "./data/Beach.jpg", False)
base64RDD.saveAsTextFile("/user/spark/spark-dil-example-03a/Beach")

# Example 03b - Import a local binary file to a RDD encoded into several parts (or splits on disk) of Base 64 text format, then save the RDD into the cluster:
base64RDD = importFromLocalBinaryFileToBase64RDD(sc, "./data/Beach.jpg", True)
base64RDD.saveAsTextFile("/user/spark/spark-dil-example-03b/Beach")

# Example 03c - Import a local binary file to a RDD compressed and encoded into several parts (or splits on disk) of Base 64 text format, then save the RDD into the cluster:
tmpZipFilePath = zipSingleLocalFile("./data/Beach.jpg", "/tmp", True) # We create first a zip file, temporary embedded in a specific parent directory
base64RDD = importFromLocalBinaryFileToBase64RDD(sc, tmpZipFilePath, True) # Import as a RDD
base64RDD.saveAsTextFile("/user/spark/spark-dil-example-03c/Beach") # Save the RDD to the cluster
deleteTmpLocalFile(tmpZipFilePath, True) # Delete the temporary zip file with its specific parent directory

# Example 04a - Import a local binary file to a binary file into the cluster, the file in the cluster encoded into one part (one block on disk) of Base 64 text format:
importFromLocalBinaryFileToClusterBase64File(sc, "./data/Beach.jpg", "/user/spark/spark-dil-example-04a/Beach", False)

# Example 04b - Import a local binary file to a binary file into the cluster, the file in the cluster is encoded into several parts (or splits on disk) of Base 64 text format:
importFromLocalBinaryFileToClusterBase64File(sc, "./data/Beach.jpg", "/user/spark/spark-dil-example-04b/Beach", True)

# Example 04c - Import a local binary file to a binary file into the cluster, the file in the cluster is compressed and encoded into several parts (or splits on disk) of Base 64 text format:
tmpZipFilePath = zipSingleLocalFile("./data/Beach.jpg", "/tmp", True) # We create first a zip file, temporary embedded in a specific parent directory
importFromLocalBinaryFileToClusterBase64File(sc, tmpZipFilePath, "/user/spark/spark-dil-example-04c/Beach", True)
deleteTmpLocalFile(tmpZipFilePath, True) # Delete the temporary zip file with its specific parent directory

# Example 05  - Import bytes from local memory to a RDD (before we load the bytes from a local file), then save the RDD into the cluster:
dataBytes = loadBytesFromLocalFile("./data/t8-shakespeare.txt")
bytesRDD = importFromLocalMemoryToRDD(sc, dataBytes, '\n') # Because of the data are lines of text, we split the content with '\n'
bytesRDD.saveAsTextFile("/user/spark/spark-dil-example-05/t8-shakespeare")

# Example 06  - Import bytes from local memory to a file into the cluster (before we load the bytes from a local file):
dataBytes = loadBytesFromLocalFile("./data/t8-shakespeare.txt")
bytesRDD = importFromLocalMemoryToClusterTextFile(sc, dataBytes, "/user/spark/spark-dil-example-06/t8-shakespeare", '\n') # Because of the data are lines of text, we split the content with '\n'

# Example 07  - Import bytes from local memory to a RDD encoded into a Base 64 text format (before we load the bytes from a local file):
dataBytes = loadBytesFromLocalFile("./data/Beach.jpg")
dataBytes = zipLocalMemory("Beach.jpg", dataBytes) # We decide before to compress the bytes
base64RDD = importFromLocalMemoryToBase64RDD(sc, dataBytes, True)
base64RDD.saveAsTextFile("/user/spark/spark-dil-example-07/Beach")

# Example 08  - Import bytes from local memory to a cluster file encoded into Base 64 text format (before we load the bytes from a local file):
dataBytes = loadBytesFromLocalFile("./data/Beach.jpg")
dataBytes = zipLocalMemory("Beach.jpg", dataBytes) # We decide before to compress the bytes
importFromLocalMemoryToClusterBase64File(sc, dataBytes, "/user/spark/spark-dil-example-08/Beach", True)
```

## Export examples
```
# Example 09  - Export from a cluster text file to a local text file (with a space added at the end of each element; the whole text is retuned in one block):
exportFromClusterTextFileToLocalTextFile(sc, "/user/spark/spark-dil-example-06/t8-shakespeare", "./spark-dil-example-09-t8-shakespeare.txt", ' ')

# Example 10  - Export from a cluster text file to a local text file (with a newLine added at the end of each element):
exportFromClusterTextFileToLocalTextFile(sc, "/user/spark/spark-dil-example-06/t8-shakespeare", "./spark-dil-example-10-t8-shakespeare.txt", '\n')

# Example 11  - Export from a cluster text file to local memory:
dataBytes = exportFromClusterTextFileToLocalMemory(sc, "/user/spark/spark-dil-example-06/t8-shakespeare", '\n')
saveBytesToLocalFile(dataBytes, "./spark-dil-example-11-t8-shakespeare.txt")

# Example 12  - Export from a cluster binary file (encoded in Base 64 text format) to a local binary file:
exportFromClusterBase64FileToLocalBinaryFile(sc, "/user/spark/spark-dil-example-08/Beach", "./spark-dil-example-12-Beach.zip")
unzipFirstItemFromLocalFile("./spark-dil-example-12-Beach.zip", "./")
##deleteFile("./spark-dil-example-12-Beach.zip")

# Example 13  - Export from a cluster binary file (encoded in Base 64 text format) to local memory:
dataBytes = exportFromClusterBase64FileToLocalMemory(sc, "/user/spark/spark-dil-example-08/Beach")
dataBytes = unzipFirstItemFromLocalMemory(dataBytes)
saveBytesToLocalFile(dataBytes, "./spark-dil-example-13-Beach.jpg")
```

Have fun :-)

To get the Spark-DIL library, please contact Philippe ROSSIGNOL at enahwe@gmail.com

Thank you
