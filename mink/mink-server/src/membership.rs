use std::sync::{Arc, RwLock};
use std::time::Duration;

use mink_core::{Member, Role};
use mink_store::Store;
use tokio::sync::watch;
use uuid::Uuid;

use crate::Error;
use crate::owners::owner;
use crate::services::Services;

pub(crate) struct Membership {
    id: Uuid,
    members: RwLock<Vec<Member>>,
}

impl Membership {
    pub(crate) fn new(id: Uuid) -> Membership {
        Membership {
            id,
            members: RwLock::new(Vec::new()),
        }
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }

    pub(crate) fn indexers(&self) -> Vec<Uuid> {
        self.with_role(Role::Indexer)
            .iter()
            .map(Member::id)
            .collect()
    }

    pub(crate) fn remote_owner(&self, path: &str) -> Option<Member> {
        let s3_members = self.with_role(Role::S3);
        let ids: Vec<Uuid> = s3_members.iter().map(Member::id).collect();

        let owner = owner(&ids, path).filter(|owner| *owner != self.id)?;
        s3_members.into_iter().find(|member| member.id() == owner)
    }

    pub(crate) async fn renew(
        &self,
        store: &Store,
        member: &Member,
        ttl: Duration,
    ) -> Result<(), Error> {
        let members = store.members();

        members.register(member, ttl).await?;
        let live = members.live().await?;

        *self
            .members
            .write()
            .expect("the member list is never poisoned") = live;
        Ok(())
    }

    fn with_role(&self, role: Role) -> Vec<Member> {
        let members = self
            .members
            .read()
            .expect("the member list is never poisoned");

        members
            .iter()
            .filter(|member| member.has(role))
            .cloned()
            .collect()
    }
}

pub(crate) async fn run(services: Arc<Services>, member: Member, mut stop: watch::Receiver<bool>) {
    let ttl = services.config.lease_ttl;

    while !*stop.borrow() {
        tokio::select! {
            _ = tokio::time::sleep(ttl / 3) => {}
            changed = stop.changed() => {
                if changed.is_err() {
                    break;
                }
                continue;
            }
        }

        let renewed = services
            .membership
            .renew(&services.store, &member, ttl)
            .await;
        if let Err(error) = renewed {
            tracing::warn!(%error, "member registration failed");
        }
    }

    if let Err(error) = services.store.members().remove(member.id()).await {
        tracing::warn!(%error, "member row was not removed");
    }
}
