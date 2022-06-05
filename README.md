# Take home assignment for Flipp

Streams data from source CSV over Apache Kafka. the consumer script reads the data and makes computations to calculate average time spent per flyer id

**Assumptions for the algorithm**

The flyer time starts when the event tag equals "flyer_open" and stops when the user performs any other action



Usage
-------------------

**Requires Docker and Docker Compose.**

```
git clone https://github.com/rahul1205/flipp.git
```

**Start the Kafka broker**

```
docker-compose up
```

**Install requrired dependencies**

```
pip install -r requirements.txt
```
**Start a consumer**

To start a consumer for processing from the stream "test":

```
python bin/processStream.py my-stream
```
**Produce data stream**

Send data from data.csv to topic “test”

```
python create_stream.py data.csv test
```
**Shut down and clean up**

Stop the consumer with Return and Ctrl+C.

Shutdown Kafka broker system:

```
docker-compose down
```
