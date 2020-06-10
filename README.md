# Simple streaming project

## The GoogleCloudPubSub code is borrowed from: https://github.com/sohailRa/beamAvroPubSub

This project demonstrates a Python Beam Streaming Pipeline for AVRO records.

The project uses Google Pub/Sub emulator with slight modification in the "cloud-client" library. The emulator can be started along with the required topic names and subscriptions by invoking the right scripts from the bin directory.

For setup:

```
pip install -r requirements.txt
```

You may need to set up the Google Cloud SDK: https://formulae.brew.sh/cask/google-cloud-sdk or https://cloud.google.com/sdk/docs/quickstart-macos for some more information.   

To start the Pub/Sub emulator cd to bin directory and run
```
./run.sh
```
To initialize the Pub/Sub run the following scripts
```
./start_pubsub_emulator.sh
./create_topics_subs.sh
./custom_consumer.sh
./custom_publisher.sh
```
And, once you are done. Kill the consumer and the streaming application by pressing CTRL+C and invoke the cleanup script from the bin directory
```
cleanup.sh
```