use mink_core::Topic;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Listing {
    topics: Vec<Topic>,
    unavailable: Vec<String>,
}

impl Listing {
    pub(crate) fn new(topics: Vec<Topic>, unavailable: Vec<String>) -> Listing {
        Listing {
            topics,
            unavailable,
        }
    }

    pub fn topics(&self) -> &[Topic] {
        &self.topics
    }

    pub fn find(&self, name: &str) -> Option<&Topic> {
        self.topics.iter().find(|topic| topic.name() == name)
    }

    pub fn unavailable(&self, name: &str) -> bool {
        self.unavailable
            .iter()
            .any(|unavailable| unavailable == name)
    }
}
