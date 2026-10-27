const SCHEMA_FIELDS: usize = 10_000;
const PAYLOAD_ITEMS: usize = 1 << 16;
const DEPTH: usize = 64;
pub(crate) const SCHEMA_TOO_LARGE: &str =
    "the schema maps to more than 10000 fields or nests deeper than 64";

pub(crate) struct Limits {
    items_left: usize,
    depth_left: usize,
}

impl Limits {
    pub(crate) fn schema() -> Limits {
        Limits {
            items_left: SCHEMA_FIELDS,
            depth_left: DEPTH,
        }
    }

    pub(crate) fn payload(length: usize) -> Limits {
        Limits {
            items_left: length + PAYLOAD_ITEMS,
            depth_left: DEPTH,
        }
    }

    pub(crate) fn take(&mut self, items: usize) -> Option<()> {
        self.items_left = self.items_left.checked_sub(items)?;

        Some(())
    }

    pub(crate) fn descend(&mut self) -> Option<()> {
        self.depth_left = self.depth_left.checked_sub(1)?;

        Some(())
    }

    pub(crate) fn ascend(&mut self) {
        self.depth_left += 1;
    }
}
