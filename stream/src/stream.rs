use std::boxed::Box;

pub struct SourceElement {
    pub id: Box<str>,
    pub data: Box<[u8]>
}

pub trait StreamProducer {
    fn produce(&self, consumer: & mut impl StreamConsumer);
}

pub trait StreamConsumer {
    fn write(&mut self, element: SourceElement);
    fn flush(&mut self);
}
