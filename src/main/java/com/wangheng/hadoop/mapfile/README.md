MapFile是经过排序过的SequenceFile, this is consisted of two model: date and index
A map is a directory containing two files, the date file, containing all keys and values in the map.
And s smaller index file, containing a fraction of of the keys.
The fraction is determined by MapFile. Writer.getIndexInterval().
