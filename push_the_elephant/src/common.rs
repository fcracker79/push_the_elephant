use std::boxed::Box;
use std::error;

/// The message that is going to be moved from PostGreSQL to Kafka
#[derive(Debug)]
pub struct SourceElement {
    /// The message id
    pub id: Box<str>,
    /// The data in the message
    pub data: Box<[u8]>
}

/// Produces [SourceElement](struct.SourceElement.html) objects from its origin and sends them to a [StreamConsumer](trait.StreamConsumer.html)
pub trait StreamProducer {
    /// Produces SourceElement objects from its origin and sends them to the consumer
    ///
    /// # Arguments:
    ///
    /// * `&self` - The [StreamProducer](trait.StreamProducer.html) itself
    /// * `consumer` - The [StreamConsumer](trait.StreamConsumer.html) that will receive messages
    fn produce(&self, consumer: & mut impl StreamConsumer) -> Result<(), Box<error::Error>>;
}

/// Sends [SourceElement](struct.SourceElement.html) objects to its destination
pub trait StreamConsumer {
    /// Sends the specified [SourceElement](struct.SourceElement.html) object to its destination
    ///
    /// # Arguments:
    /// `&mut self` - the consumer itself
    /// `element` - the [SourceElement](struct.SourceElement.html) to be written
    fn write(&mut self, element: SourceElement) -> Result<(), Box<error::Error>>;
    /// Flushes all the messages that have been received
    ///
    /// # Arguments:
    /// `&mut self` - the consumer itself
    fn flush(&mut self) -> Result<(), Box<error::Error>>;
}
