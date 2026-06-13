# TODO

- Separate processes for emitting and consuming events like https://github.com/litestar-org/litestar-saq and https://github.com/hasansezertasan/litestar-faststream.
- Serialization like FastStream?
- Should we use a managed queue (single queue for all events) or separate queues for each event type (queue_name = event_ids) on RabbitMQ and the like? Which one is expected for Litestar Event System?
- Other Backends:
  - https://github.com/ag2ai/faststream/issues/794
  - https://github.com/ag2ai/faststream/issues/1142
  - https://github.com/ag2ai/faststream/issues/1229
