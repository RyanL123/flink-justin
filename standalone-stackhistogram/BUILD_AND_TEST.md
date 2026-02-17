# How to Build and Test the Standalone Stack Histogram Module

## Option 1: Using Maven (Recommended)

### Prerequisites
- Java 8 or higher
- Maven 3.6+

### Build and Test

```bash
cd standalone-stackhistogram

# Compile the code
mvn compile

# Run all tests
mvn test

# Run tests with verbose output
mvn test -X

# Clean and rebuild
mvn clean compile test
```

### Expected Output

When tests pass, you should see:
```
[INFO] Tests run: 15, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

---

## Option 2: Using Plain Java Compiler

### Prerequisites
- Java 8 or higher
- JUnit 5 JAR file

### Step 1: Download JUnit 5

Download `junit-platform-console-standalone-1.9.2.jar` from:
https://repo1.maven.org/maven2/org/junit/platform/junit-platform-console-standalone/1.9.2/

Or use wget:
```bash
cd standalone-stackhistogram
wget https://repo1.maven.org/maven2/org/junit/platform/junit-platform-console-standalone/1.9.2/junit-platform-console-standalone-1.9.2.jar
```

### Step 2: Compile

```bash
cd standalone-stackhistogram

# Compile main class
javac StackHistogram.java

# Compile test class (requires JUnit JAR on classpath)
javac -cp "junit-platform-console-standalone-1.9.2.jar" StackHistogramTest.java
```

### Step 3: Run Tests

```bash
# Run all tests
java -jar junit-platform-console-standalone-1.9.2.jar \
    --class-path . \
    --scan-class-path \
    --include-classname=StackHistogramTest
```

---

## Option 3: Quick Test Without JUnit

Create a simple test file:

```bash
cat > SimpleTest.java << 'EOF'
import java.util.*;

public class SimpleTest {
    public static void main(String[] args) {
        // Create sample histograms
        Map<Integer, Long> buckets1 = new HashMap<>();
        buckets1.put(0, 100L);
        buckets1.put(1, 50L);
        
        Map<Integer, Long> buckets2 = new HashMap<>();
        buckets2.put(0, 80L);
        buckets2.put(1, 60L);
        
        StackHistogram h1 = new StackHistogram(buckets1, 500L);
        StackHistogram h2 = new StackHistogram(buckets2, 500L);
        
        // Merge
        StackHistogram merged = StackHistogram.merge(List.of(h1, h2));
        
        // Verify results
        System.out.println("Bucket 0: " + merged.getFrequency(0)); // Should be 180
        System.out.println("Bucket 1: " + merged.getFrequency(1)); // Should be 110
        System.out.println("Total partitions: " + merged.getNumPartitions()); // Should be 2
        
        if (merged.getFrequency(0) == 180L && merged.getFrequency(1) == 110L) {
            System.out.println("✓ Test PASSED!");
        } else {
            System.out.println("✗ Test FAILED!");
        }
    }
}
EOF

# Compile and run
javac StackHistogram.java SimpleTest.java
java SimpleTest
```

---

## What the Tests Verify

The test suite (`StackHistogramTest.java`) includes:

1. **Basic Operations**:
   - Creating histograms
   - Getting frequencies for specific buckets
   - Calculating total frequency

2. **Merging Logic**:
   - Merging 2 histograms (sums corresponding buckets)
   - Merging 3+ histograms
   - Merging histograms with disjoint buckets
   - Merging empty histograms

3. **Validation**:
   - Rejects empty list
   - Rejects histograms with different `numBuckets`
   - Uses max `maxStackDistance` when merging

4. **Edge Cases**:
   - Single histogram merge (returns same)
   - Many histograms (10+)
   - Partition count preservation

---

## Troubleshooting

### Issue: "package org.junit.jupiter.api does not exist"
**Solution**: Make sure JUnit 5 JAR is on the classpath when compiling tests.

### Issue: "cannot find symbol: class StackHistogram"
**Solution**: Compile `StackHistogram.java` first, or compile both files together:
```bash
javac *.java
```

### Issue: Maven not found
**Solution**: Install Maven or use Option 2 (plain Java compiler).

### Issue: Java version mismatch
**Solution**: Ensure Java 8+ is installed:
```bash
java -version
```
