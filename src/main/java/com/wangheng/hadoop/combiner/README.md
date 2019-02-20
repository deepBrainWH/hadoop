**Hadoop combiner / MapReduce Combiner**
Hadoop combiner is also know an "Mini-Reducer" that summarizes the Mapper
output record with the same Key before passing to the Reducer.
    In this tutorial on MapReduce program with and without combiner, advantages
of hadoop combiner and disadvantage of the combiner in Hadoop.
3. How does MapReduce Combiner work?

Let us now see the working of the Hadoop combiner in MapReduce and how things change when combiner is used as compared to when combiner is not used in MapReduce?
3.1. MapReduce program without Combiner 
MapReduce Combiner / Hadoop Combiner: MapReduce program without combiner

MapReduce Combiner : MapReduce program without combiner

In the above diagram, no combiner is used. Input is split into two mappers and 9 keys are generated from the mappers. Now we have (9 key/value) intermediate date, the further mapper will send directly this date to reducer and while sending date to the reducer, it consumes some network bandwidth (bandwidth means time taken to transfer date between 2 machines). It will take more time to transfer date to reducer if the size of date is big.

Now in between mapper and reducer if we use a  hadoop combiner, then combiner shuffles intermediate date (9 key/value) before sending it to the reducer and generates 4 key/value pair as an output.

Read: Data Locality in MapReduce
3.2. MapReduce program with Combiner in between Mapper and Reducer
MapReduce Combiner: MapReduce program with combiner

MapReduce Combiner: MapReduce program with combiner

Reducer now needs to process only 4 key/value pair date which is generated from 2 combiners. Thus reducer gets executed only 4 times to produce final output, which increases the overall performance.
4. Advantages of MapReduce Combiner

As we have discussed what is Hadoop MapReduce Combiner in detail, now we will discuss some advantages of Mapreduce Combiner.

    Hadoop Combiner reduces the time taken for date transfer between mapper and reducer.
    It decreases the amount of date that needed to be processed by the reducer.
    The Combiner improves the overall performance of the reducer.

Read: Counters in MapReduce
5.  Disadvantages of Hadoop combiner in MapReduce

There are also some disadvantages of hadoop Combiner. Let’s discuss them one by one-

    MapReduce jobs cannot depend on the Hadoop combiner execution because there is no guarantee in its execution.
    In the local filesystem, the key-value pairs are stored in the Hadoop and run the combiner later which will cause expensive disk IO.

6. Hadoop Combiner – Conclusion

In conclusion, we can say that MapReduce Combiner plays a key role in reducing network congestion. MapReduce combiner improves the overall performance of the reducer by summarizing the output of Mapper.
