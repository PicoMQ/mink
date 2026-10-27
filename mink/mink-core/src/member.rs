use std::fmt;

use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    Catalog,
    S3,
    Indexer,
    Admin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    id: Uuid,
    roles: Vec<Role>,
    address: Option<String>,
    admin_address: Option<String>,
}

impl Role {
    pub const ALL: [Role; 4] = [Role::Catalog, Role::S3, Role::Indexer, Role::Admin];

    pub fn name(&self) -> &'static str {
        match self {
            Role::Catalog => "catalog",
            Role::S3 => "s3",
            Role::Indexer => "indexer",
            Role::Admin => "admin",
        }
    }

    pub fn from_name(name: &str) -> Option<Role> {
        Role::ALL.into_iter().find(|role| role.name() == name)
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl Member {
    pub fn new(
        id: Uuid,
        roles: Vec<Role>,
        address: Option<String>,
        admin_address: Option<String>,
    ) -> Member {
        Member {
            id,
            roles,
            address,
            admin_address,
        }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn roles(&self) -> &[Role] {
        &self.roles
    }

    pub fn has(&self, role: Role) -> bool {
        self.roles.contains(&role)
    }

    pub fn address(&self) -> Option<&str> {
        self.address.as_deref()
    }

    pub fn admin_address(&self) -> Option<&str> {
        self.admin_address.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_parse_from_their_names() {
        for role in Role::ALL {
            assert_eq!(Role::from_name(role.name()), Some(role));
            assert_eq!(role.to_string(), role.name());
        }

        assert_eq!(Role::from_name("writer"), None);
    }
}
