use kafka_protocol::protocol::Request;

use crate::Error;

pub(crate) const NONE: i16 = 0;
pub(crate) const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub(crate) const LEADER_NOT_AVAILABLE: i16 = 5;
pub(crate) const NOT_LEADER_OR_FOLLOWER: i16 = 6;
pub(crate) const UNSUPPORTED_VERSION: i16 = 35;
pub(crate) const FENCED_LEADER_EPOCH: i16 = 74;
pub(crate) const UNKNOWN_LEADER_EPOCH: i16 = 75;

pub(crate) fn check<R: Request>(code: i16) -> Result<(), Error> {
    match code {
        NONE => Ok(()),
        UNSUPPORTED_VERSION => Err(Error::UnsupportedVersion { key: R::KEY }),
        code => Err(Error::Broker { code }),
    }
}
