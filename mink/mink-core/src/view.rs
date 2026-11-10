#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum View {
    Merged,
    Stream,
    Lake,
}

impl View {
    pub const ALL: [View; 3] = [View::Merged, View::Stream, View::Lake];

    pub fn name(self) -> &'static str {
        match self {
            View::Merged => "merged",
            View::Stream => "stream",
            View::Lake => "lake",
        }
    }

    pub fn parse(name: &str) -> Option<View> {
        View::ALL.into_iter().find(|view| view.name() == name)
    }

    pub(crate) fn suffix(self) -> String {
        match self {
            View::Merged => String::new(),
            other => format!("-{}", other.name()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn views_parse_from_their_names_only() {
        for view in View::ALL {
            assert_eq!(View::parse(view.name()), Some(view));
        }

        assert_eq!(View::parse("kafka"), None);
        assert_eq!(View::parse("Merged"), None);
    }
}
