use std::boxed::Box;

/// The message that is going to be moved from PostGreSQL to Kafka
#[derive(Debug)]
pub struct SourceElement {
    /// The message id
    pub id: Box<str>,
    /// The data in the message
    pub data: Box<[u8]>
}

/// Produces [SourceElement](stream.SourceElement.thml) objects from its origin and sends them to a [StreamConsumer](stream.StreamConsumer.html)
pub trait StreamProducer {
    /// Produces SourceElement objects from its origin and sends them to the consumer
    ///
    /// # Arguments:
    ///
    /// * `&self` - The [StreamProducer](stream.StreamProducer.html) itself
    /// * `consumer` - The [StreamConsumer](stream.StreamConsumer.html) that will receive messages
    fn produce(&self, consumer: & mut impl StreamConsumer);
}

/// Sends [SourceElement](stream.SourceElement.thml) objects to its destination
pub trait StreamConsumer {
    /// Sends the specified [SourceElement](stream.SourceElement.thml) object to its destination
    ///
    /// # Arguments:
    /// `&mut self` - the consumer itself
    /// `element` - the [SourceElement](stream.SourceElement.thml) to be written
    fn write(&mut self, element: SourceElement);
    /// Flushes all the messages that have been received
    ///
    /// # Arguments:
    /// `&mut self` - the consumer itself
    fn flush(&mut self);
}
