//! Dependency requirements and failure policies for the worker.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Requirement {
    Required,
    Optional,
    OnUse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailurePolicy {
    /// Stop accepting work until the required component recovers.
    BlockWork,
    /// Use local cache or exchange lookup if shared caching fails.
    Fallback,
    /// Publish failure propagates when this optional workflow is invoked.
    FailOperation,
}

#[derive(Debug)]
pub struct DependencyContract {
    pub name: &'static str,
    pub requirement: Requirement,
    pub failure_policy: FailurePolicy,
}

pub fn dependency_contracts() -> Vec<DependencyContract> {
    let mut contracts: Vec<_> = [
        "mongodb.bot",
        "mongodb.tradeStation",
        "mongodb.executionClaims",
        "rabbitmq.connection",
        "rabbitmq.publisher",
        "rabbitmq.consumer.bingxFutures",
        "redis.locks",
    ]
    .into_iter()
    .map(|name| DependencyContract {
        name,
        requirement: Requirement::Required,
        failure_policy: FailurePolicy::BlockWork,
    })
    .collect();
    contracts.push(DependencyContract {
        name: "redis.exchangeMetadataCache",
        requirement: Requirement::Optional,
        failure_policy: FailurePolicy::Fallback,
    });
    // Accepted-signal/command publication remains upstream. Pub/Sub failures
    // propagate only when the notification workflow is invoked.
    contracts.push(DependencyContract {
        name: "redis.notifications",
        requirement: Requirement::OnUse,
        failure_policy: FailurePolicy::FailOperation,
    });
    contracts
}
