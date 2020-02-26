# Lockless data structure for high throughput, low latency multiple producer / consumer system
Message buffer organised as two dimensional array for high throughput and latency.
The implementation uses only atomics. Note that even atomics could be a performance bottleneck as this data structure demonstrates.
Very likely the performance could be further improved through better CAS API than the ones used here. Yet to experiment that!
See documentation.pdf for some more details and analysis.

MBuffer.h - producer consumer code

MsgQExample.cpp - example usage

MBufferStats.cpp - performance stats using MBuffer.h

documentation.pdf - analysis of performance
