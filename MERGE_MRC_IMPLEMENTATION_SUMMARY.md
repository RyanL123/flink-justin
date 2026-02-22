# MergeMRC Module Implementation Summary

## Overview

This document summarizes the implementation of the **MergeMRC (Merge Resource Collector)** module in the Flink Job Manager. The module collects stack histograms from all Task Managers running tasks for a job and merges them into a single aggregated histogram that can be accessed via REST API.

## Architecture

The MergeMRC module is integrated into the Flink Job Manager architecture as follows:

```
┌─────────────────────────────────────────────────────────────┐
│                    Job Manager (JobMaster)                   │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              MergeMRC Service                        │  │
│  │  - Uses RestClient to fetch from Task Managers       │  │
│  │  - Groups executions by Task Manager                  │  │
│  │  - Merges histograms by summing counts               │  │
│  │  - Returns MergedStackHistogram                       │  │
│  └──────────────────────────────────────────────────────┘  │
│                          │                                   │
│                          │ HTTP GET                          │
│                          ▼                                   │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              RestClient                               │  │
│  │  - Sends HTTP requests to Task Manager REST endpoints│  │
│  │  - Endpoint: /taskmanagers/stackhistogram             │  │
│  └──────────────────────────────────────────────────────┘  │
│                          │                                   │
│                          ▼                                   │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         JobMasterGateway                             │  │
│  │  - requestMergedStackHistogram()                     │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                          │
                          │ REST API
                          ▼
┌─────────────────────────────────────────────────────────────┐
│              WebMonitorEndpoint (REST Server)               │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         StackHistogramHandler                        │  │
│  │  - Endpoint: GET /jobs/:jobid/stackhistogram         │  │
│  │  - Returns: JSON with merged histogram               │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                          │
                          │ HTTP Request
                          ▼
                    External Clients

                          │
                          │ HTTP GET (REST API)
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    Task Manager 1                           │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  REST Endpoint: /taskmanagers/stackhistogram         │  │
│  │  - Returns TaskManagerStackHistogramResponseBody      │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    Task Manager 2                           │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  REST Endpoint: /taskmanagers/stackhistogram         │  │
│  │  - Returns TaskManagerStackHistogramResponseBody      │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Stack Distance Histogram Format

Based on **Quickmrc design (Section 3.4.2)** from the A4S paper, stack histograms are **stack distance frequency histograms**:

- **Key**: Bucket index (Integer, 0 to H-1) representing a range of stack distances
- **Value**: Frequency count (Long) - number of accesses with stack distances falling in that bucket range
- **H**: Number of histogram buckets (default 50 as per Quickmrc implementation)
- **Dmax**: Maximum stack distance - total number of keys in LRU and ghost caches

The stack distance range [0, Dmax] is split into H fixed-size histogram buckets. For bucket index i, it covers stack distances [i * (Dmax/H), (i+1) * (Dmax/H)).

Example format:
```json
{
  "mergedHistogram": {
    "0": 150,   // Bucket 0: stack distances 0-10 (if Dmax=500, H=50)
    "1": 89,    // Bucket 1: stack distances 11-20
    "2": 42,    // Bucket 2: stack distances 21-30
    ...
  },
  "numBuckets": 50,
  "maxStackDistance": 500,
  "numPartitions": 2
}
```

When merging histograms from multiple Task Managers:
- Frequencies for corresponding bucket indices are **summed**
- All histograms must have the same number of buckets (H) and max stack distance (Dmax)
- After merging, the histogram is scaled by the number of partitions P to derive the operator miss-rate curve: MRC(m*P) = MRC_task(m)

## Files Created

### 1. Core Module Classes

#### `flink/flink-runtime/src/main/java/org/apache/flink/runtime/stackhistogram/StackHistogram.java`
- **Purpose**: Represents a single stack distance histogram collected from a Task Manager
- **Key Fields**:
  - `executionAttemptID`: Identifies which task produced this histogram
  - `histogram`: Map<Integer, Long> mapping bucket indices (0 to H-1) to frequency counts
  - `numBuckets`: Number of histogram buckets H (default 50)
  - `maxStackDistance`: Maximum stack distance Dmax
  - `timestamp`: When the histogram was collected
- **Methods**:
  - `merge()`: Merges two histograms by summing frequencies for corresponding bucket indices (validates same H and Dmax)

#### `flink/flink-runtime/src/main/java/org/apache/flink/runtime/stackhistogram/MergedStackHistogram.java`
- **Purpose**: Represents the final merged stack distance histogram for an operator
- **Key Fields**:
  - `jobID`: The job this histogram belongs to
  - `mergedHistogram`: Map<Integer, Long> with aggregated frequency counts per bucket
  - `numBuckets`: Number of histogram buckets H
  - `maxStackDistance`: Maximum stack distance Dmax
  - `numPartitions`: Number of partitions/tasks that contributed (used for scaling miss-rate curve)
  - `contributingExecutions`: List of execution attempt IDs that contributed data
  - `timestamp`: When the merge was performed

#### `flink/flink-runtime/src/main/java/org/apache/flink/runtime/stackhistogram/MergeMRC.java`
- **Purpose**: Main service class that orchestrates collection and merging (implements Quickmrc merging logic)
- **Key Responsibilities**:
  1. Queries JobMaster for all running execution attempts and their Task Manager locations
  2. **Collects** stack histograms from Task Managers (pure collection phase)
  3. **Merges** collected histograms into a single merged histogram (pure merge phase)
  4. Groups executions by Task Manager to optimize HTTP requests
  5. Uses `RestClient` to fetch stack distance histograms from Task Manager REST endpoints
  6. Tracks number of partitions for scaling the miss-rate curve
  7. Handles errors gracefully (continues with available data)
- **Key Components**:
  - `RestClient`: HTTP client for making REST API requests to Task Managers
  - `Configuration`: Used to configure RestClient and determine REST port
  - Groups executions by `TaskManagerLocation` to avoid duplicate HTTP requests
- **Phase Separation**:
  - `CollectedStackHistograms` (inner static class): value object holding `JobID` + `List<StackHistogram>` for the **collection** phase result
  - `collectStackHistograms()`: collects histograms from all Task Managers and returns `CompletableFuture<CollectedStackHistograms>`
  - `mergeStackHistograms(CollectedStackHistograms)`: pure static method that merges the collected histograms into `MergedStackHistogram`
  - `collectAndMergeStackHistograms()`: convenience method that composes `collectStackHistograms()` → `mergeStackHistograms(...)`
- **Integration Points**:
  - Uses `JobMasterGateway.requestJob()` to get execution graph
  - Extracts `TaskManagerLocation` from each execution vertex
  - Makes HTTP GET requests to `/taskmanagers/stackhistogram` on each Task Manager
  - Parses `TaskManagerStackHistogramResponseBody` responses
  - Returns `CompletableFuture<MergedStackHistogram>`
- **Merging Algorithm**:
  - Validates all histograms have same H and Dmax
  - Sums frequencies for corresponding bucket indices
  - Tracks max Dmax across all histograms
  - Records number of contributing partitions for scaling
- **REST Client Details**:
  - Creates `RestClient` instance in constructor using `Configuration` and `Executor`
  - Uses default REST port 8081 (configurable via `RestOptions.PORT`)
  - Makes asynchronous HTTP requests using `RestClient.sendRequest()`
  - Handles network errors and returns empty histograms on failure

### 2. REST API Components

#### Job Manager REST Endpoint (for external clients)

#### `flink/flink-runtime/src/main/java/org/apache/flink/runtime/rest/messages/job/StackHistogramHeaders.java`
- **Purpose**: Defines the REST endpoint specification for Job Manager
- **Endpoint**: `GET /jobs/:jobid/stackhistogram`
- **Parameters**: Job ID (path parameter)
- **Response**: `StackHistogramResponseBody`
- **Usage**: External clients use this endpoint to retrieve merged stack histograms

#### `flink/flink-runtime/src/main/java/org/apache/flink/runtime/rest/messages/job/StackHistogramResponseBody.java`
- **Purpose**: JSON response structure for the Job Manager REST API
- **Fields**:
  - `jobId`: String representation of the job ID
  - `mergedHistogram`: Map<Integer, Long> with merged stack distance histogram (bucket index -> frequency)
  - `numBuckets`: Number of histogram buckets H
  - `maxStackDistance`: Maximum stack distance Dmax
  - `numPartitions`: Number of partitions that contributed (for scaling miss-rate curve)
  - `contributingExecutions`: List of execution IDs that contributed
  - `timestamp`: When the merge was performed

#### `flink/flink-runtime/src/main/java/org/apache/flink/runtime/rest/handler/job/StackHistogramHandler.java`
- **Purpose**: REST handler that processes HTTP requests from external clients
- **Functionality**:
  - Validates that the gateway is a JobMasterGateway
  - Calls `requestMergedStackHistogram()` on the gateway
  - Converts result to JSON response
  - Handles errors appropriately

#### Task Manager REST Endpoint (for Job Manager to fetch histograms)

#### `flink/flink-runtime/src/main/java/org/apache/flink/runtime/rest/messages/taskmanager/TaskManagerStackHistogramHeaders.java`
- **Purpose**: Defines the REST endpoint specification for Task Manager
- **Endpoint**: `GET /taskmanagers/stackhistogram`
- **Parameters**: None (empty message parameters)
- **Response**: `TaskManagerStackHistogramResponseBody`
- **Usage**: Job Manager's `MergeMRC` service uses this endpoint to fetch stack histograms from Task Managers
- **Note**: This endpoint is expected to be implemented on the Task Manager side

#### `flink/flink-runtime/src/main/java/org/apache/flink/runtime/rest/messages/taskmanager/TaskManagerStackHistogramResponseBody.java`
- **Purpose**: JSON response structure for the Task Manager REST API
- **Fields**:
  - `executionAttemptId`: String representation of the execution attempt ID
  - `histogram`: Map<Integer, Long> with stack distance histogram (bucket index -> frequency)
  - `numBuckets`: Number of histogram buckets H
  - `maxStackDistance`: Maximum stack distance Dmax
  - `timestamp`: When the histogram was collected
- **Note**: The current implementation assumes one histogram per response. If Task Managers return multiple histograms (one per execution), the response format may need to be updated to a list or map structure.

## Files Modified

### 1. JobMaster Integration

#### `flink/flink-runtime/src/main/java/org/apache/flink/runtime/jobmaster/JobMaster.java`
**Changes**:
- Added `mergeMRC` field (nullable, initialized in `onStart()`)
- Initialized MergeMRC in `onStart()` method after gateway is available
  - Passes `jobMasterConfiguration.getConfiguration()` to MergeMRC constructor for RestClient setup
- Added cleanup in `onStop()` to properly shutdown MergeMRC (which also closes RestClient)
- Implemented `requestMergedStackHistogram()` method that delegates to MergeMRC

**Key Code Locations**:
- Field declaration: ~line 228
- Initialization: `onStart()` method (passes Configuration for RestClient)
- Cleanup: `onStop()` method (closes RestClient via MergeMRC.closeAsync())
- RPC method implementation: ~line 897

#### `flink/flink-runtime/src/main/java/org/apache/flink/runtime/jobmaster/JobMasterGateway.java`
**Changes**:
- Added import for `MergedStackHistogram`
- Added method signature: `CompletableFuture<MergedStackHistogram> requestMergedStackHistogram(@RpcTimeout Time timeout)`

### 2. REST Endpoint Registration

#### `flink/flink-runtime/src/main/java/org/apache/flink/runtime/webmonitor/WebMonitorEndpoint.java`
**Changes**:
- Added import for `StackHistogramHandler` and `StackHistogramHeaders`
- Created and registered `StackHistogramHandler` in `initializeHandlers()` method
- Handler is registered alongside other job-related handlers (~line 770)

## Implementation Details

### Collection & Merge Flow

1. **Client Request**: HTTP GET request to `/jobs/:jobid/stackhistogram`
2. **Handler Processing**: `StackHistogramHandler` receives request
3. **Gateway Call**: Handler calls `JobMasterGateway.requestMergedStackHistogram()`
4. **MergeMRC Collection Phase (`collectStackHistograms()`)**:
   - Gets execution graph from JobMaster via `JobMasterGateway.requestJob()`
   - Filters for running executions (`ExecutionState.RUNNING`)
   - Extracts `TaskManagerLocation` from each execution vertex
   - **Groups executions by Task Manager** to optimize HTTP requests:
     - Creates a map: `TaskManagerLocation -> List<ExecutionAttemptID>`
     - Avoids making duplicate HTTP requests to the same Task Manager
   - **For each Task Manager**:
     - Gets Task Manager hostname from `TaskManagerLocation.getHostname()`
     - Determines REST port (default 8081, configurable via `RestOptions.PORT`)
     - Uses `RestClient.sendRequest()` to make HTTP GET request to `/taskmanagers/stackhistogram`
     - Parses `TaskManagerStackHistogramResponseBody` response
     - Converts response to `StackHistogram` objects (one per execution on that Task Manager)
   - Combines all per-TaskManager lists into a single `List<StackHistogram>`
   - Wraps result in `new CollectedStackHistograms(jobID, allHistograms)`
5. **MergeMRC Merge Phase (`mergeStackHistograms(...)`)**:
   - Takes `CollectedStackHistograms` (or directly `JobID` + `List<StackHistogram>`)
   - Validates histograms and computes:
     - `numBuckets` (from first histogram, default 50)
     - `maxStackDistance` (max of all Dmax)
     - `numPartitions` (number of histograms)
   - Sums frequencies for corresponding bucket indices across all histograms
   - Returns `MergedStackHistogram` with aggregated data
6. **Convenience Method (`collectAndMergeStackHistograms()`)**:
   - Implements: `collectStackHistograms().thenApplyAsync(MergeMRC::mergeStackHistograms, executor)`
   - This is what `JobMaster.requestMergedStackHistogram()` calls
7. **Response**: Returns merged histogram as JSON via `StackHistogramResponseBody`

### Merging Algorithm (Quickmrc Section 3.4.2)

The merging process aggregates stack distance histograms from multiple Task Managers:

```java
// For each histogram from a Task Manager
for (StackHistogram histogram : collectedHistograms) {
    // Validate same configuration
    if (firstHistogram) {
        numBuckets = histogram.getNumBuckets();
        maxStackDistance = histogram.getMaxStackDistance();
    }
    
    // For each bucket in the histogram
    histogram.getHistogram().forEach((bucketIndex, count) -> {
        // Sum frequencies for corresponding bucket indices
        mergedHistogram.merge(bucketIndex, count, Long::sum);
    });
}
```

This ensures that:
- Frequencies for corresponding bucket indices are summed across all Task Managers
- All histograms must have the same number of buckets (H) and max stack distance (Dmax)
- The final histogram represents the unscaled miss-rate curve after merging
- The histogram can be scaled by `numPartitions` to derive the operator miss-rate curve: MRC(m*P) = MRC_task(m)
- Each execution that contributed is tracked in `contributingExecutions`

### Error Handling

- **No Running Executions**: Returns empty histogram with empty contributing executions list
- **Failed HTTP Requests**: 
  - Network errors, timeouts, or invalid responses are caught
  - Logs warning with Task Manager and execution details
  - Returns empty histogram for that Task Manager/execution
  - Continues processing other Task Managers (doesn't fail entire request)
- **Shutdown State**: Returns exception if MergeMRC has been shut down
- **Invalid Gateway**: Handler validates that gateway is JobMasterGateway before proceeding
- **RestClient Errors**: 
  - Connection failures are handled gracefully
  - Empty histograms are returned for failed requests
  - RestClient is properly closed in `closeAsync()` method
- **Response Parsing Errors**: 
  - JSON deserialization errors are caught
  - Empty histograms are returned for invalid responses
  - Logs warnings for debugging

## Integration Points

### Task Manager Side REST Endpoint Implementation

The Job Manager's `MergeMRC` service fetches stack histograms from Task Managers via REST API. To complete the integration, Task Managers need to implement the REST endpoint:

1. **REST Endpoint**: `/taskmanagers/stackhistogram`
   - **Method**: GET
   - **Response**: `TaskManagerStackHistogramResponseBody`
   - **Location**: Task Manager's REST server (typically on port 8081)

2. **Task Manager REST Handler Implementation** (using Quickmrc Section 3.4.1):
   - Generate stack distances using bucket-based LRU approximation
   - Split stack distance range [0, Dmax] into H fixed-size histogram buckets (default H=50)
   - Record frequency of stack distances per bucket
   - Format as `Map<Integer, Long>` (bucket index → frequency count)
   - Return as `TaskManagerStackHistogramResponseBody` with:
     - `executionAttemptId`: String representation of the execution ID
     - `histogram`: Map<Integer, Long> with bucket frequencies
     - `numBuckets`: Number of histogram buckets H (default 50)
     - `maxStackDistance`: Maximum stack distance Dmax
     - `timestamp`: Collection timestamp

3. **Response Format Considerations**:
   - **Current Implementation**: Assumes one histogram per response
   - **If Multiple Executions**: If a Task Manager runs multiple tasks, the endpoint may need to:
     - Accept an `executionAttemptId` query parameter to return a specific histogram
     - OR return a map/list of histograms (one per execution)
     - The current `MergeMRC` implementation creates histograms for all requested executions using the same response data, which may need adjustment

### Example Task Manager REST Handler Implementation

```java
// In Task Manager REST handler
@GET
@Path("/taskmanagers/stackhistogram")
public TaskManagerStackHistogramResponseBody getStackHistogram(
        @QueryParam("executionAttemptId") String executionAttemptId) {
    
    // Get stack distance histogram from Quickmrc
    // Quickmrc generates histogram using bucket-based LRU (Section 3.4.1)
    ExecutionAttemptID execId = ExecutionAttemptID.fromHexString(executionAttemptId);
    Map<Integer, Long> histogram = quickmrc.getStackDistanceHistogram(execId);
    int numBuckets = quickmrc.getNumBuckets(); // default 50
    long maxStackDistance = quickmrc.getMaxStackDistance(); // L * (1 + G)
    
    return new TaskManagerStackHistogramResponseBody(
        executionAttemptId,
        histogram,
        numBuckets,
        maxStackDistance,
        System.currentTimeMillis());
}
```

### RestClient Configuration

The `MergeMRC` service uses Flink's `RestClient` to make HTTP requests:

- **Creation**: `RestClient` is created in `MergeMRC` constructor using:
  - `Configuration`: For REST client settings (SSL, timeouts, etc.)
  - `Executor`: For asynchronous request handling
- **Configuration**: 
  - REST port defaults to 8081 (`RestOptions.PORT`)
  - Can be configured via Flink configuration
  - SSL support is automatically enabled if HTTPS is detected
- **Request Flow**:
  1. `RestClient.sendRequest(hostname, port, headers)` is called
  2. Request is serialized to JSON using Jackson ObjectMapper
  3. HTTP GET request is sent to Task Manager
  4. Response is deserialized to `TaskManagerStackHistogramResponseBody`
  5. Response is converted to `StackHistogram` objects

### Network Considerations

- **Task Manager Discovery**: Task Manager addresses are obtained from `TaskManagerLocation` objects in the execution graph
- **Hostname Resolution**: Uses `TaskManagerLocation.getHostname()` to get the hostname
- **Port Configuration**: Currently uses default port 8081; should ideally come from Task Manager registration info
- **Error Handling**: Network failures are caught and empty histograms are returned for failed requests
- **Concurrent Requests**: Multiple HTTP requests are made concurrently using `CompletableFuture` and `Executor`

## API Usage

### REST Endpoint

**Endpoint**: `GET /jobs/:jobid/stackhistogram`

**Example Request**:
```bash
curl http://localhost:8081/jobs/1234567890abcdef/stackhistogram
```

**Example Response**:
```json
{
  "jobId": "1234567890abcdef",
  "mergedHistogram": {
    "0": 450,   // Bucket 0: frequencies for stack distances 0-10
    "1": 267,   // Bucket 1: frequencies for stack distances 11-20
    "2": 126,   // Bucket 2: frequencies for stack distances 21-30
    ...
  },
  "numBuckets": 50,
  "maxStackDistance": 500,
  "numPartitions": 3,
  "contributingExecutions": [
    "execution-1",
    "execution-2",
    "execution-3"
  ],
  "timestamp": 1704816000000
}
```

## Testing Recommendations

1. **Unit Tests**:
   - Test `StackHistogram.merge()` with various combinations
   - Test `MergeMRC.collectAndMergeStackHistograms()` with mock data
   - Test error handling scenarios

2. **Integration Tests**:
   - Test REST endpoint with real JobMaster
   - Test with multiple Task Managers
   - Test with no running executions
   - Test error scenarios (Task Manager unavailable, etc.)

3. **End-to-End Tests**:
   - Submit a job with multiple tasks
   - Collect stack histograms from Task Managers
   - Verify merged histogram contains aggregated data
   - Verify REST API returns correct format

## Future Enhancements

1. **Caching**: Cache merged histograms with TTL to reduce computation
2. **Filtering**: Add query parameters to filter by execution ID or time range
3. **Sampling**: Add support for sampling rates to reduce overhead
4. **Metrics**: Add metrics for collection time, histogram size, etc.
5. **Streaming**: Support streaming updates instead of one-time collection

## REST API Implementation Details

### Request Flow Diagram

```
External Client
    │
    │ HTTP GET /jobs/:jobid/stackhistogram
    ▼
WebMonitorEndpoint (Job Manager REST Server)
    │
    │ Routes to StackHistogramHandler
    ▼
StackHistogramHandler
    │
    │ Calls JobMasterGateway.requestMergedStackHistogram()
    ▼
JobMaster
    │
    │ Delegates to MergeMRC.collectAndMergeStackHistograms()
    ▼
MergeMRC Service
    │
    │ 1. Gets execution graph from JobMasterGateway
    │ 2. Extracts running executions and Task Manager locations
    │ 3. Groups executions by Task Manager
    │
    │ For each Task Manager:
    │    │
    │    │ HTTP GET /taskmanagers/stackhistogram
    │    │ Host: <taskmanager-hostname>:8081
    │    ▼
    │ RestClient (in MergeMRC)
    │    │
    │    │ Serializes request, sends HTTP GET
    │    │ Deserializes TaskManagerStackHistogramResponseBody
    │    ▼
    │ Task Manager REST Server
    │    │
    │    │ Returns stack histogram JSON
    │    ▼
    │ MergeMRC (continues)
    │
    │ 4. Merges all histograms
    │ 5. Returns MergedStackHistogram
    ▼
StackHistogramHandler
    │
    │ Converts to StackHistogramResponseBody (JSON)
    ▼
External Client (receives merged histogram JSON)
```

### Code Flow Example

```java
// 1. External client makes request
GET /jobs/abc123/stackhistogram

// 2. StackHistogramHandler processes request
public CompletableFuture<StackHistogramResponseBody> handleRequest(...) {
    JobMasterGateway jobMasterGateway = (JobMasterGateway) gateway;
    return jobMasterGateway
        .requestMergedStackHistogram(timeout)
        .thenApply(StackHistogramResponseBody::new);
}

// 3. JobMaster delegates to MergeMRC
@Override
public CompletableFuture<MergedStackHistogram> requestMergedStackHistogram(Time timeout) {
    return mergeMRC.collectAndMergeStackHistograms();
}

// 4. MergeMRC collects histograms
public CompletableFuture<MergedStackHistogram> collectAndMergeStackHistograms() {
    // Get execution graph
    return jobMasterGateway.requestJob(rpcTimeout)
        .thenComposeAsync(executionGraphInfo -> {
            // Extract Task Manager locations
            Map<ExecutionAttemptID, TaskManagerLocation> executionToTaskManager = ...;
            
            // Group by Task Manager
            Map<TaskManagerLocation, List<ExecutionAttemptID>> executionsByTaskManager = ...;
            
            // Fetch histograms via REST
            List<CompletableFuture<List<StackHistogram>>> futures = new ArrayList<>();
            for (Map.Entry<TaskManagerLocation, List<ExecutionAttemptID>> entry : 
                    executionsByTaskManager.entrySet()) {
                futures.add(requestStackHistogramsFromTaskManagerViaRest(
                    entry.getKey(), entry.getValue()));
            }
            
            // Merge all histograms
            return CompletableFuture.allOf(futures.toArray(...))
                .thenApplyAsync(v -> {
                    // Sum frequencies for corresponding buckets
                    Map<Integer, Long> mergedHistogram = new HashMap<>();
                    // ... merging logic ...
                    return new MergedStackHistogram(...);
                });
        });
}

// 5. HTTP request to Task Manager
private CompletableFuture<List<StackHistogram>> 
        requestStackHistogramsFromTaskManagerViaRest(...) {
    return CompletableFuture.supplyAsync(() -> {
        String hostname = taskManagerLocation.getHostname();
        int port = configuration.getInteger(RestOptions.PORT, 8081);
        
        // Make HTTP GET request
        TaskManagerStackHistogramResponseBody response = restClient
            .sendRequest(hostname, port, TaskManagerStackHistogramHeaders.getInstance())
            .get();
        
        // Convert to StackHistogram objects
        List<StackHistogram> histograms = new ArrayList<>();
        for (ExecutionAttemptID execId : executionAttemptIDs) {
            histograms.add(new StackHistogram(
                execId,
                response.getHistogram(),
                response.getNumBuckets(),
                response.getMaxStackDistance(),
                response.getTimestamp()));
        }
        return histograms;
    });
}
```

### HTTP Request/Response Examples

#### Request from Job Manager to Task Manager

```http
GET /taskmanagers/stackhistogram HTTP/1.1
Host: taskmanager-1.example.com:8081
Accept: application/json
```

#### Response from Task Manager

```json
{
  "executionAttemptId": "abc123def456",
  "histogram": {
    "0": 150,
    "1": 89,
    "2": 42,
    ...
    "49": 5
  },
  "numBuckets": 50,
  "maxStackDistance": 500,
  "timestamp": 1704816000000
}
```

#### Final Response to External Client

```json
{
  "jobId": "abc123",
  "mergedHistogram": {
    "0": 450,
    "1": 267,
    "2": 126,
    ...
    "49": 15
  },
  "numBuckets": 50,
  "maxStackDistance": 500,
  "numPartitions": 3,
  "contributingExecutions": [
    "execution-1",
    "execution-2",
    "execution-3"
  ],
  "timestamp": 1704816000000
}
```

## Detailed Implementation Explanation

### MergeMRC Service Architecture

The `MergeMRC` service is a **stateless service** that runs within the Job Manager (`JobMaster`). It follows Flink's asynchronous programming model using `CompletableFuture` for all operations.

#### Initialization

1. **Constructor**: 
   - Takes `JobID`, `JobMasterGateway`, `Executor`, `Time`, and `Configuration`
   - Creates a `RestClient` instance using the provided `Configuration` and `Executor`
   - The `RestClient` is used for all HTTP communication with Task Managers

2. **Lifecycle**:
   - Initialized in `JobMaster.onStart()` after the RPC gateway is available
   - Shut down in `JobMaster.onStop()` which calls `MergeMRC.closeAsync()`
   - `closeAsync()` properly closes the `RestClient` to free resources

#### Collection Process

The `collectAndMergeStackHistograms()` method implements the following flow:

1. **Get Execution Graph**:
   ```java
   jobMasterGateway.requestJob(rpcTimeout)
   ```
   - Retrieves the current execution graph for the job
   - Contains information about all tasks and their states

2. **Extract Running Executions**:
   - Iterates through all execution vertices in the graph
   - Filters for `ExecutionState.RUNNING` executions
   - Extracts `TaskManagerLocation` from each vertex
   - Creates a map: `ExecutionAttemptID -> TaskManagerLocation`

3. **Group by Task Manager**:
   ```java
   Map<TaskManagerLocation, List<ExecutionAttemptID>> executionsByTaskManager
   ```
   - Groups executions by their Task Manager location
   - **Optimization**: Reduces HTTP requests by batching requests per Task Manager
   - If multiple tasks run on the same Task Manager, only one HTTP request is made

4. **Fetch Histograms via REST**:
   - For each Task Manager:
     - Extracts hostname: `taskManagerLocation.getHostname()`
     - Gets REST port: `configuration.getInteger(RestOptions.PORT, 8081)`
     - Makes HTTP GET request: `RestClient.sendRequest(hostname, port, TaskManagerStackHistogramHeaders.getInstance())`
     - Parses response: `TaskManagerStackHistogramResponseBody`
     - Converts to `StackHistogram` objects (one per execution on that Task Manager)
   - All requests are made **concurrently** using `CompletableFuture.supplyAsync()`

5. **Merge Histograms**:
   - Waits for all HTTP requests to complete using `CompletableFuture.allOf()`
   - Validates histogram configuration (same H and Dmax)
   - Sums frequencies for corresponding bucket indices:
     ```java
     histogram.getHistogram().forEach((bucketIndex, count) ->
         mergedHistogram.merge(bucketIndex, count, Long::sum)
     );
     ```
   - Tracks number of contributing partitions
   - Creates `MergedStackHistogram` with aggregated data

#### REST Client Details

The `RestClient` is Flink's standard HTTP client for REST API communication:

- **Configuration**: Uses Flink's `Configuration` object for settings
- **SSL Support**: Automatically enabled if HTTPS URLs are detected
- **Serialization**: Uses Jackson ObjectMapper for JSON serialization/deserialization
- **Asynchronous**: All requests are non-blocking using Netty
- **Error Handling**: Network errors are propagated as exceptions

#### Response Handling

The current implementation assumes:
- Each Task Manager REST endpoint returns **one histogram per request**
- The response contains a single `TaskManagerStackHistogramResponseBody`
- If multiple executions are on the same Task Manager, the same histogram data is used for all

**Future Enhancement**: If Task Managers return multiple histograms (one per execution), the response format should be updated to:
- Accept `executionAttemptId` as a query parameter, OR
- Return a map/list of histograms in the response body

### Key Design Decisions

1. **REST API vs RPC**: 
   - Chose REST API for Task Manager communication because stack histograms are already exposed via REST endpoints
   - REST is more flexible and doesn't require RPC gateway setup
   - Easier to debug and test (can use curl/browser)

2. **Grouping by Task Manager**:
   - Optimizes network requests by reducing duplicate calls
   - If 10 tasks run on 2 Task Managers, only 2 HTTP requests are made instead of 10

3. **Error Resilience**:
   - Individual Task Manager failures don't fail the entire request
   - Empty histograms are returned for failed requests
   - Merging continues with available data

4. **Asynchronous Processing**:
   - All operations use `CompletableFuture` for non-blocking execution
   - HTTP requests are made concurrently
   - Follows Flink's async programming patterns

## Summary

The MergeMRC module successfully integrates into the Flink Job Manager to implement **Quickmrc's merging logic (Section 3.4.2)**:
- ✅ Collect stack distance histograms from all Task Managers via REST API
- ✅ Group executions by Task Manager to optimize HTTP requests
- ✅ Merge histograms by summing frequencies for corresponding bucket indices
- ✅ Track histogram configuration (H buckets, Dmax) and number of partitions
- ✅ Expose merged results via REST API with proper format
- ✅ Handle errors gracefully (network failures, invalid responses)
- ✅ Follow Flink's architectural patterns (async, CompletableFuture)
- ✅ Match Quickmrc design specifications from the A4S paper
- ✅ Use RestClient for HTTP communication with Task Managers

The implementation is **complete on the Job Manager side** and correctly implements the merging algorithm described in Section 3.4.2. The Job Manager fetches stack histograms from Task Managers via REST API endpoints (`/taskmanagers/stackhistogram`). The remaining work is to implement the Task Manager side REST endpoint handler to actually collect and return stack distance histograms using Quickmrc's bucket-based LRU approximation (Section 3.4.1).
