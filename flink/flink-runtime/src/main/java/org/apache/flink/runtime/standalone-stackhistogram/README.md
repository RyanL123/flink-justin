# Standalone Stack Histogram Module

A standalone Java module for merging stack distance histograms based on the **Quickmrc design (Section 3.4.2)** from the A4S paper.

## Overview

This module provides a pure implementation of stack histogram merging logic without any dependencies on Flink or other frameworks. It can be used independently to merge stack distance histograms from multiple sources.

## Stack Distance Histogram Format

Based on **Quickmrc design (Section 3.4.2)**, stack histograms are **stack distance frequency histograms**:

- **Key**: Bucket index (Integer, 0 to H-1) representing a range of stack distances
- **Value**: Frequency count (Long) - number of accesses with stack distances falling in that bucket range
- **H**: Number of histogram buckets (default 50 as per Quickmrc implementation)
- **Dmax**: Maximum stack distance - total number of keys in LRU and ghost caches

The stack distance range [0, Dmax] is split into H fixed-size histogram buckets. For bucket index i, it covers stack distances [i * (Dmax/H), (i+1) * (Dmax/H)).

## Usage

### Creating a Stack Histogram

```java
import com.stackhistogram.StackHistogram;
import java.util.Map;

// Create a histogram with bucket frequencies
Map<Integer, Long> buckets = new HashMap<>();
buckets.put(0, 100L);  // Bucket 0: 100 accesses
buckets.put(1, 50L);   // Bucket 1: 50 accesses
buckets.put(2, 25L);   // Bucket 2: 25 accesses

// Create histogram with default 50 buckets
StackHistogram histogram = new StackHistogram(buckets, 500L); // Dmax = 500

// Or specify all parameters
StackHistogram histogram2 = new StackHistogram(
    buckets, 
    50,    // numBuckets
    500L,  // maxStackDistance
    1      // numPartitions
);
```

### Merging Histograms

```java
import java.util.List;

// Create multiple histograms
StackHistogram h1 = new StackHistogram(Map.of(0, 100L, 1, 50L), 500L);
StackHistogram h2 = new StackHistogram(Map.of(0, 80L, 1, 60L), 500L);
StackHistogram h3 = new StackHistogram(Map.of(0, 120L, 1, 40L), 500L);

// Merge using static method
StackHistogram merged = StackHistogram.merge(List.of(h1, h2, h3));

// Or merge two histograms using instance method
StackHistogram merged2 = h1.merge(h2);

// Access merged results
long bucket0Frequency = merged.getFrequency(0);  // 300 (100 + 80 + 120)
long bucket1Frequency = merged.getFrequency(1);  // 150 (50 + 60 + 40)
int numPartitions = merged.getNumPartitions();   // 3
```

### Merging Algorithm

The merging process (Quickmrc Section 3.4.2):

1. **Validation**: All histograms must have the same `numBuckets` (H)
2. **Max Stack Distance**: The merged histogram uses the maximum `maxStackDistance` (Dmax) across all input histograms
3. **Frequency Summing**: For each bucket index, frequencies are summed across all histograms
4. **Partition Count**: The total number of partitions is the sum of all input partition counts

## Requirements

- Java 8 or higher
- JUnit 5 (for tests)

## Building

```bash
# Compile
javac StackHistogram.java

# Compile tests (requires JUnit 5 on classpath)
javac -cp "junit-platform-console-standalone.jar" StackHistogramTest.java

# Run tests
java -jar junit-platform-console-standalone.jar --class-path . --scan-class-path
```

Or use Maven:
```bash
mvn compile
mvn test
```

## Example

See `StackHistogramTest.java` for comprehensive examples with sample data.

## License

This is a standalone implementation for educational/research purposes.
