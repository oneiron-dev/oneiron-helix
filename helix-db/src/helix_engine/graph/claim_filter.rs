use crate::protocol::value::Value;
use crate::utils::items::Node;

#[derive(Debug, Clone)]
pub struct ClaimFilterConfig {
    pub require_approved: bool,
    pub require_active: bool,
    pub exclude_stale: bool,
    pub allowed_approval_statuses: Vec<String>,
}

impl Default for ClaimFilterConfig {
    fn default() -> Self {
        Self {
            require_approved: true,
            require_active: true,
            exclude_stale: true,
            allowed_approval_statuses: vec!["auto".into(), "approved".into()],
        }
    }
}

impl ClaimFilterConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn disabled() -> Self {
        Self {
            require_approved: false,
            require_active: false,
            exclude_stale: false,
            allowed_approval_statuses: vec![],
        }
    }

    pub fn with_require_approved(mut self, require: bool) -> Self {
        self.require_approved = require;
        self
    }

    pub fn with_require_active(mut self, require: bool) -> Self {
        self.require_active = require;
        self
    }

    pub fn with_exclude_stale(mut self, exclude: bool) -> Self {
        self.exclude_stale = exclude;
        self
    }

    pub fn with_allowed_approval_statuses(mut self, statuses: Vec<String>) -> Self {
        self.allowed_approval_statuses = statuses;
        self
    }
}

pub fn passes_claim_filter(node: &Node, config: &ClaimFilterConfig) -> bool {
    if config.require_approved && !has_approval_status(node, &config.allowed_approval_statuses) {
        return false;
    }

    if config.require_active && !is_lifecycle_active(node) {
        return false;
    }

    if config.exclude_stale && is_stale(node) {
        return false;
    }

    true
}

fn get_string_property<'a>(node: &'a Node, key: &str) -> Option<&'a str> {
    match node.get_property(key) {
        Some(Value::String(s)) => Some(s),
        _ => None,
    }
}

fn has_approval_status(node: &Node, allowed_statuses: &[String]) -> bool {
    get_string_property(node, "approvalStatus")
        .map(|status| allowed_statuses.iter().any(|s| s == status))
        .unwrap_or(false)
}

fn is_lifecycle_active(node: &Node) -> bool {
    get_string_property(node, "lifecycleStatus") == Some("active")
}

fn is_stale(node: &Node) -> bool {
    matches!(node.get_property("stale"), Some(Value::Boolean(true)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::properties::ImmutablePropertiesMap;
    use bumpalo::Bump;
    use std::collections::HashMap;

    struct ClaimNodeBuilder<'arena> {
        arena: &'arena Bump,
        approval_status: Option<&'static str>,
        lifecycle_status: Option<&'static str>,
        stale: Option<bool>,
    }

    impl<'arena> ClaimNodeBuilder<'arena> {
        fn new(arena: &'arena Bump) -> Self {
            Self {
                arena,
                approval_status: None,
                lifecycle_status: None,
                stale: None,
            }
        }

        fn approval_status(mut self, status: &'static str) -> Self {
            self.approval_status = Some(status);
            self
        }

        fn lifecycle_status(mut self, status: &'static str) -> Self {
            self.lifecycle_status = Some(status);
            self
        }

        fn stale(mut self, stale: bool) -> Self {
            self.stale = Some(stale);
            self
        }

        fn build(self) -> Node<'arena> {
            let mut props = HashMap::new();

            if let Some(status) = self.approval_status {
                props.insert(
                    self.arena.alloc_str("approvalStatus") as &str,
                    Value::String(status.to_string()),
                );
            }
            if let Some(status) = self.lifecycle_status {
                props.insert(
                    self.arena.alloc_str("lifecycleStatus") as &str,
                    Value::String(status.to_string()),
                );
            }
            if let Some(stale) = self.stale {
                props.insert(
                    self.arena.alloc_str("stale") as &str,
                    Value::Boolean(stale),
                );
            }

            let len = props.len();
            let properties = ImmutablePropertiesMap::new(len, props.into_iter(), self.arena);
            Node {
                id: 1,
                label: self.arena.alloc_str("claim"),
                version: 0,
                properties: Some(properties),
            }
        }
    }

    fn valid_claim(arena: &Bump) -> ClaimNodeBuilder {
        ClaimNodeBuilder::new(arena)
            .approval_status("auto")
            .lifecycle_status("active")
            .stale(false)
    }

    #[test]
    fn test_default_config() {
        let config = ClaimFilterConfig::default();
        assert!(config.require_approved);
        assert!(config.require_active);
        assert!(config.exclude_stale);
        assert_eq!(config.allowed_approval_statuses, vec!["auto", "approved"]);
    }

    #[test]
    fn test_disabled_config() {
        let config = ClaimFilterConfig::disabled();
        assert!(!config.require_approved);
        assert!(!config.require_active);
        assert!(!config.exclude_stale);
    }

    #[test]
    fn test_passes_claim_filter_approved_auto() {
        let arena = Bump::new();
        let node = valid_claim(&arena).build();
        assert!(passes_claim_filter(&node, &ClaimFilterConfig::default()));
    }

    #[test]
    fn test_passes_claim_filter_approved_approved() {
        let arena = Bump::new();
        let node = valid_claim(&arena).approval_status("approved").build();
        assert!(passes_claim_filter(&node, &ClaimFilterConfig::default()));
    }

    #[test]
    fn test_excludes_unapproved() {
        let arena = Bump::new();
        let node = valid_claim(&arena).approval_status("pending").build();
        assert!(!passes_claim_filter(&node, &ClaimFilterConfig::default()));
    }

    #[test]
    fn test_excludes_stale() {
        let arena = Bump::new();
        let node = valid_claim(&arena).stale(true).build();
        assert!(!passes_claim_filter(&node, &ClaimFilterConfig::default()));
    }

    #[test]
    fn test_excludes_inactive() {
        let arena = Bump::new();
        let node = valid_claim(&arena).lifecycle_status("superseded").build();
        assert!(!passes_claim_filter(&node, &ClaimFilterConfig::default()));
    }

    #[test]
    fn test_missing_approval_status() {
        let arena = Bump::new();
        let node = ClaimNodeBuilder::new(&arena)
            .lifecycle_status("active")
            .stale(false)
            .build();
        assert!(!passes_claim_filter(&node, &ClaimFilterConfig::default()));
    }

    #[test]
    fn test_missing_lifecycle_status() {
        let arena = Bump::new();
        let node = ClaimNodeBuilder::new(&arena)
            .approval_status("auto")
            .stale(false)
            .build();
        assert!(!passes_claim_filter(&node, &ClaimFilterConfig::default()));
    }

    #[test]
    fn test_missing_stale_defaults_to_not_stale() {
        let arena = Bump::new();
        let node = ClaimNodeBuilder::new(&arena)
            .approval_status("auto")
            .lifecycle_status("active")
            .build();
        assert!(passes_claim_filter(&node, &ClaimFilterConfig::default()));
    }

    #[test]
    fn test_disabled_config_allows_all() {
        let arena = Bump::new();
        let node = ClaimNodeBuilder::new(&arena)
            .approval_status("rejected")
            .lifecycle_status("superseded")
            .stale(true)
            .build();
        assert!(passes_claim_filter(&node, &ClaimFilterConfig::disabled()));
    }

    #[test]
    fn test_partial_config() {
        let arena = Bump::new();
        let node = valid_claim(&arena).approval_status("pending").build();
        let config = ClaimFilterConfig::default().with_require_approved(false);
        assert!(passes_claim_filter(&node, &config));
    }

    #[test]
    fn test_custom_approval_statuses() {
        let arena = Bump::new();
        let node = valid_claim(&arena).approval_status("custom").build();
        let config =
            ClaimFilterConfig::default().with_allowed_approval_statuses(vec!["custom".into()]);
        assert!(passes_claim_filter(&node, &config));
    }
}
