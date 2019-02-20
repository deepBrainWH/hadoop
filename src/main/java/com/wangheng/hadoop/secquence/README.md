**SecquenceFile是hadoop提供的一种对二进制文件的支持，
通过secquenceFile 将小文件进行合并，以减少namenode的压力**

sequenceFile 的读写操作

#### sequence File同样也支持写压缩操作，和读压缩操作，写入文件自动压缩，读出文件时自动解压