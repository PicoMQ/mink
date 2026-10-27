#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Live,
    Backfill,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request {
    Metadata,
    Offsets,
    Fetch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Head,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Object {
    Data,
    Metadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Success,
    ClientError,
    ServerError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Forward {
    Ok,
    Unreachable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    Config,
    Namespaces,
    Tables,
    Load,
    Write,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    Ok,
    Missing,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Worker {
    Indexer,
    Writer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Partition,
    Topic,
}

impl Source {
    pub(crate) const ALL: [Source; 2] = [Source::Live, Source::Backfill];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Source::Live => "live",
            Source::Backfill => "backfill",
        }
    }
}

impl Request {
    pub(crate) const ALL: [Request; 3] = [Request::Metadata, Request::Offsets, Request::Fetch];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Request::Metadata => "metadata",
            Request::Offsets => "offsets",
            Request::Fetch => "fetch",
        }
    }
}

impl Outcome {
    pub(crate) const ALL: [Outcome; 2] = [Outcome::Ok, Outcome::Error];

    pub fn of<T, E>(result: &Result<T, E>) -> Outcome {
        match result {
            Ok(_) => Outcome::Ok,
            Err(_) => Outcome::Error,
        }
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Outcome::Ok => "ok",
            Outcome::Error => "error",
        }
    }
}

impl Method {
    pub(crate) const ALL: [Method; 2] = [Method::Get, Method::Head];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Method::Get => "get",
            Method::Head => "head",
        }
    }
}

impl Object {
    pub(crate) const ALL: [Object; 2] = [Object::Data, Object::Metadata];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Object::Data => "data",
            Object::Metadata => "metadata",
        }
    }
}

impl Status {
    pub(crate) const ALL: [Status; 3] = [Status::Success, Status::ClientError, Status::ServerError];

    pub fn of(code: u16) -> Status {
        match code {
            500.. => Status::ServerError,
            400.. => Status::ClientError,
            _ => Status::Success,
        }
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Status::Success => "2xx",
            Status::ClientError => "4xx",
            Status::ServerError => "5xx",
        }
    }
}

impl Forward {
    pub(crate) const ALL: [Forward; 2] = [Forward::Ok, Forward::Unreachable];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Forward::Ok => "ok",
            Forward::Unreachable => "unreachable",
        }
    }
}

impl Route {
    pub(crate) const ALL: [Route; 5] = [
        Route::Config,
        Route::Namespaces,
        Route::Tables,
        Route::Load,
        Route::Write,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Route::Config => "config",
            Route::Namespaces => "namespaces",
            Route::Tables => "tables",
            Route::Load => "load",
            Route::Write => "write",
        }
    }
}

impl Answer {
    pub(crate) const ALL: [Answer; 3] = [Answer::Ok, Answer::Missing, Answer::Error];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Answer::Ok => "ok",
            Answer::Missing => "missing",
            Answer::Error => "error",
        }
    }
}

impl Worker {
    pub(crate) const ALL: [Worker; 2] = [Worker::Indexer, Worker::Writer];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Worker::Indexer => "indexer",
            Worker::Writer => "writer",
        }
    }
}

impl Scope {
    pub(crate) const ALL: [Scope; 2] = [Scope::Partition, Scope::Topic];

    pub(crate) fn label(self) -> &'static str {
        match self {
            Scope::Partition => "partition",
            Scope::Topic => "topic",
        }
    }
}
