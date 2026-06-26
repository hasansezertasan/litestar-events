# TODO

- Separate processes for emitting and consuming events like https://github.com/litestar-org/litestar-saq and https://github.com/hasansezertasan/litestar-faststream.
- Serialization like FastStream?
- Should we use a managed queue (single queue for all events) or separate queues for each event type (queue_name = event_ids) on RabbitMQ and the like? Which one is expected for Litestar Event System?
  - Decision (SQS backend): single managed queue for all events, with the
    originating `event_id` carried in a message attribute. This is the
    precedent for future managed backends.
- Other Backends:
  - [x] AWS SQS — https://github.com/ag2ai/faststream/issues/794 (`contrib/sqs`)
  - [x] GCP Pub/Sub — https://github.com/ag2ai/faststream/issues/1229 (`contrib/pubsub`)
  - [x] ZeroMQ — https://github.com/ag2ai/faststream/issues/1142 (`contrib/zmq`)
