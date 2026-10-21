use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedLabels {
    pub npm: Vec<NpmHost>,
    pub cf_dns: Option<CloudflareDns>,
    pub gatus: Vec<GatusMonitor>,
    /// Diagnostic messages for labels that could not be parsed. The
    /// parsing function never fails; it logs problems here so callers can
    /// surface them without breaking unrelated integrations.
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NpmHost {
    pub index: u8,
    /// One or more public domain names that resolve to this single NPM proxy
    /// host. Written as a comma-separated `host` label; a single domain is the
    /// common case. NPM stores these as one entry's `domain_names`.
    pub hosts: Vec<String>,
    pub port: u16,
    pub scheme: NpmScheme,
    pub ssl: bool,
    pub hsts: Option<bool>,
    pub hsts_subdomains: Option<bool>,
    pub http2: Option<bool>,
    pub block_exploits: Option<bool>,
    pub websockets: Option<bool>,
    pub cert_id: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NpmScheme {
    Http,
    Https,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudflareDns {
    pub name: String,
    pub target: String,
    pub proxied: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatusMonitor {
    pub index: u8,
    pub url: String,
    pub interval: String,
    pub conditions: Vec<String>,
    pub group: Option<String>,
    pub name: Option<String>,
    pub telegram: bool,
    /// `None` when the label is absent; [`Severity::DEFAULT`] applies then.
    pub severity: Option<Severity>,
    /// `None` when the label is absent; [`Area::DEFAULT`] applies then.
    pub area: Option<Area>,
}

/// How urgent an alert from a Gatus endpoint is, for the alert service that
/// reads the classification map.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Critical,
    Warning,
    Info,
}

impl Severity {
    pub const DEFAULT: Severity = Severity::Warning;

    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "critical" => Ok(Self::Critical),
            "warning" => Ok(Self::Warning),
            "info" => Ok(Self::Info),
            other => Err(format!(
                "severity must be critical|warning|info, got '{other}'"
            )),
        }
    }
}

/// Which part of the estate a Gatus endpoint watches, for the alert service
/// that reads the classification map.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Area {
    Core,
    Backups,
    Storage,
    Network,
    Apps,
    Deploys,
    Agents,
}

impl Area {
    pub const DEFAULT: Area = Area::Apps;

    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "core" => Ok(Self::Core),
            "backups" => Ok(Self::Backups),
            "storage" => Ok(Self::Storage),
            "network" => Ok(Self::Network),
            "apps" => Ok(Self::Apps),
            "deploys" => Ok(Self::Deploys),
            "agents" => Ok(Self::Agents),
            other => Err(format!(
                "area must be core|backups|storage|network|apps|deploys|agents, got '{other}'"
            )),
        }
    }
}

pub fn parse(labels: &BTreeMap<String, String>) -> ParsedLabels {
    let mut out = ParsedLabels {
        npm: Vec::new(),
        cf_dns: None,
        gatus: Vec::new(),
        warnings: Vec::new(),
    };

    let mut npm_groups: BTreeMap<u8, BTreeMap<String, String>> = BTreeMap::new();
    let mut gatus_groups: BTreeMap<u8, BTreeMap<String, String>> = BTreeMap::new();
    let mut cf_dns: BTreeMap<String, String> = BTreeMap::new();

    for (key, value) in labels {
        let Some(rest) = key.strip_prefix("docktornet.") else {
            continue;
        };
        let parts: Vec<&str> = rest.split('.').collect();

        match parts.as_slice() {
            ["npm", idx, field] => match parse_canonical_index(idx) {
                Ok(index) => {
                    if is_npm_field(field) {
                        npm_groups
                            .entry(index)
                            .or_default()
                            .insert((*field).to_string(), value.clone());
                    } else {
                        out.warnings.push(format!(
                            "unrecognised npm.{index}.{field} field in key '{key}'"
                        ));
                    }
                }
                Err(_) => out
                    .warnings
                    .push(format!("invalid npm index in key '{key}'")),
            },
            ["gatus", idx, field] => match parse_canonical_index(idx) {
                Ok(index) => {
                    if is_gatus_field(field) {
                        gatus_groups
                            .entry(index)
                            .or_default()
                            .insert((*field).to_string(), value.clone());
                    } else {
                        out.warnings.push(format!(
                            "unrecognised gatus.{index}.{field} field in key '{key}'"
                        ));
                    }
                }
                Err(_) => out
                    .warnings
                    .push(format!("invalid gatus index in key '{key}'")),
            },
            ["cf", "dns", field] => {
                if is_cf_dns_field(field) {
                    cf_dns.insert((*field).to_string(), value.clone());
                } else {
                    out.warnings
                        .push(format!("unrecognised cf.dns.{field} field in key '{key}'"));
                }
            }
            _ => out
                .warnings
                .push(format!("unrecognised docktornet key '{key}'")),
        }
    }

    for (index, fields) in npm_groups {
        match build_npm_host(index, &fields) {
            Ok((host, warnings)) => {
                out.npm.push(host);
                for warning in warnings {
                    out.warnings.push(format!("npm.{index}: {warning}"));
                }
            }
            Err(err) => out.warnings.push(format!("npm.{index}: {err}")),
        }
    }

    for (index, fields) in gatus_groups {
        match build_gatus(index, &fields) {
            Ok(monitor) => out.gatus.push(monitor),
            Err(err) => out.warnings.push(format!("gatus.{index}: {err}")),
        }
    }

    if !cf_dns.is_empty() {
        match build_cf_dns(&cf_dns) {
            Ok(dns) => out.cf_dns = Some(dns),
            Err(err) => out.warnings.push(format!("cf.dns: {err}")),
        }
    }

    out.npm.sort_by_key(|host| host.index);
    out.gatus.sort_by_key(|monitor| monitor.index);

    out
}

/// Parse an index token, requiring it to be in canonical decimal form so that
/// non-canonical spellings (e.g. "00", "01") cannot collide with their
/// canonical counterparts ("0", "1") and silently merge two distinct label
/// groups into one — dropping a public hostname. The u8 value is used as the
/// stable NPM host key, so it must round-trip exactly.
fn parse_canonical_index(idx: &str) -> Result<u8, ()> {
    let index = idx.parse::<u8>().map_err(|_| ())?;
    if idx == index.to_string() {
        Ok(index)
    } else {
        Err(())
    }
}

fn build_npm_host(
    index: u8,
    fields: &BTreeMap<String, String>,
) -> Result<(NpmHost, Vec<String>), String> {
    let hosts: Vec<String> = fields
        .get("host")
        .ok_or("missing host")?
        .split(',')
        .map(|host| host.trim().to_string())
        .filter(|host| !host.is_empty())
        .collect();
    if hosts.is_empty() {
        return Err("missing host".to_string());
    }
    let port = fields
        .get("port")
        .ok_or("missing port")?
        .parse::<u16>()
        .map_err(|_| "port must be a u16".to_string())?;
    let scheme = match fields.get("scheme").map(String::as_str) {
        Some("http") => NpmScheme::Http,
        Some("https") => NpmScheme::Https,
        Some(other) => return Err(format!("scheme must be http|https, got {other}")),
        None => return Err("missing scheme".to_string()),
    };
    let ssl = parse_bool(fields.get("ssl").ok_or("missing ssl")?)?;

    // Optional hardening flags are advisory: a single malformed value must
    // degrade to its default plus a warning, never un-publish the whole host.
    let mut warnings = Vec::new();
    let hsts = parse_optional_bool(fields, "hsts", &mut warnings);
    let hsts_subdomains = parse_optional_bool(fields, "hsts_subdomains", &mut warnings);
    let http2 = parse_optional_bool(fields, "http2", &mut warnings);
    let block_exploits = parse_optional_bool(fields, "block_exploits", &mut warnings);
    let websockets = parse_optional_bool(fields, "websockets", &mut warnings);
    let cert_id = match fields.get("cert_id") {
        Some(value) => match value.parse::<u32>() {
            Ok(parsed) => Some(parsed),
            Err(_) => {
                warnings.push(format!(
                    "cert_id: must be u32, got '{value}'; ignoring optional field"
                ));
                None
            }
        },
        None => None,
    };

    Ok((
        NpmHost {
            index,
            hosts,
            port,
            scheme,
            ssl,
            hsts,
            hsts_subdomains,
            http2,
            block_exploits,
            websockets,
            cert_id,
        },
        warnings,
    ))
}

fn is_npm_field(field: &str) -> bool {
    matches!(
        field,
        "host"
            | "port"
            | "scheme"
            | "ssl"
            | "hsts"
            | "hsts_subdomains"
            | "http2"
            | "block_exploits"
            | "websockets"
            | "cert_id"
    )
}

fn build_cf_dns(fields: &BTreeMap<String, String>) -> Result<CloudflareDns, String> {
    let name = fields.get("name").ok_or("missing name")?.clone();
    let target = fields.get("target").ok_or("missing target")?.clone();
    let proxied = parse_bool(fields.get("proxied").ok_or("missing proxied")?)?;

    Ok(CloudflareDns {
        name,
        target,
        proxied,
    })
}

fn is_cf_dns_field(field: &str) -> bool {
    matches!(field, "name" | "target" | "proxied")
}

fn build_gatus(index: u8, fields: &BTreeMap<String, String>) -> Result<GatusMonitor, String> {
    let url = fields.get("url").ok_or("missing url")?.clone();
    let interval = fields.get("interval").ok_or("missing interval")?.clone();
    let mut conditions: Vec<String> = match fields.get("conditions") {
        Some(conditions) => conditions
            .split(',')
            .map(|condition| condition.trim().to_string())
            .filter(|condition| !condition.is_empty())
            .collect(),
        None => vec!["[STATUS] == 200".to_string()],
    };
    if conditions.is_empty() {
        conditions.push("[STATUS] == 200".to_string());
    }

    let group = fields.get("group").cloned().filter(|g| !g.is_empty());
    let name = fields.get("name").cloned().filter(|n| !n.is_empty());
    let telegram = match fields.get("telegram") {
        Some(value) => parse_bool(value)?,
        None => true,
    };
    // An unknown value drops the monitor rather than defaulting: a typo must
    // not quietly demote a critical endpoint.
    let severity = fields
        .get("severity")
        .map(|value| Severity::parse(value))
        .transpose()?;
    let area = fields
        .get("area")
        .map(|value| Area::parse(value))
        .transpose()?;

    Ok(GatusMonitor {
        index,
        url,
        interval,
        conditions,
        group,
        name,
        telegram,
        severity,
        area,
    })
}

fn is_gatus_field(field: &str) -> bool {
    matches!(
        field,
        "url" | "interval" | "conditions" | "group" | "name" | "telegram" | "severity" | "area"
    )
}

fn parse_bool(value: impl AsRef<str>) -> Result<bool, String> {
    match value.as_ref().trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(format!("expected bool, got '{}'", value.as_ref())),
    }
}

/// Parse an optional boolean field, degrading a malformed value to `None`
/// plus a warning rather than aborting the whole host.
fn parse_optional_bool(
    fields: &BTreeMap<String, String>,
    key: &str,
    warnings: &mut Vec<String>,
) -> Option<bool> {
    match fields.get(key) {
        Some(value) => match parse_bool(value) {
            Ok(parsed) => Some(parsed),
            Err(err) => {
                warnings.push(format!("{key}: {err}; ignoring optional field"));
                None
            }
        },
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lbl(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn no_docktornet_labels_yields_empty() {
        let p = parse(&lbl(&[("foo", "bar")]));
        assert!(p.npm.is_empty());
        assert!(p.cf_dns.is_none());
        assert!(p.gatus.is_empty());
        assert!(p.warnings.is_empty());
    }

    #[test]
    fn single_npm_host_parses() {
        let p = parse(&lbl(&[
            ("docktornet.npm.0.host", "a.example.com"),
            ("docktornet.npm.0.port", "3000"),
            ("docktornet.npm.0.scheme", "http"),
            ("docktornet.npm.0.ssl", "true"),
        ]));
        assert_eq!(p.npm.len(), 1);
        let h = &p.npm[0];
        assert_eq!(h.index, 0);
        assert_eq!(h.hosts, vec!["a.example.com".to_string()]);
        assert_eq!(h.port, 3000);
        assert_eq!(h.scheme, NpmScheme::Http);
        assert!(h.ssl);
        assert_eq!(h.hsts, None);
        assert_eq!(h.hsts_subdomains, None);
        assert_eq!(h.block_exploits, None);
        assert_eq!(h.websockets, None);
        assert_eq!(h.cert_id, None);
        assert!(p.warnings.is_empty());
    }

    #[test]
    fn npm_host_splits_comma_separated_domains_into_one_entry() {
        let p = parse(&lbl(&[
            ("docktornet.npm.0.host", "a.example.com, b.example.com"),
            ("docktornet.npm.0.port", "3000"),
            ("docktornet.npm.0.scheme", "http"),
            ("docktornet.npm.0.ssl", "true"),
        ]));
        assert_eq!(p.npm.len(), 1);
        assert_eq!(
            p.npm[0].hosts,
            vec!["a.example.com".to_string(), "b.example.com".to_string()]
        );
        assert!(p.warnings.is_empty());
    }

    #[test]
    fn npm_host_with_only_separators_emits_warning_and_skips() {
        let p = parse(&lbl(&[
            ("docktornet.npm.0.host", " , , "),
            ("docktornet.npm.0.port", "3000"),
            ("docktornet.npm.0.scheme", "http"),
            ("docktornet.npm.0.ssl", "true"),
        ]));
        assert!(p.npm.is_empty());
        assert_eq!(p.warnings.len(), 1);
        assert!(p.warnings[0].contains("npm.0"));
    }

    #[test]
    fn multiple_npm_hosts_parse_by_index() {
        let p = parse(&lbl(&[
            ("docktornet.npm.0.host", "a"),
            ("docktornet.npm.0.port", "1"),
            ("docktornet.npm.0.scheme", "http"),
            ("docktornet.npm.0.ssl", "true"),
            ("docktornet.npm.2.host", "c"),
            ("docktornet.npm.2.port", "3"),
            ("docktornet.npm.2.scheme", "https"),
            ("docktornet.npm.2.ssl", "false"),
        ]));
        assert_eq!(p.npm.len(), 2);
        assert_eq!(p.npm[0].index, 0);
        assert_eq!(p.npm[1].index, 2);
    }

    #[test]
    fn npm_missing_required_field_emits_warning_and_skips() {
        let p = parse(&lbl(&[
            ("docktornet.npm.0.host", "a"),
            // missing port + scheme + ssl
        ]));
        assert!(p.npm.is_empty());
        assert_eq!(p.warnings.len(), 1);
        assert!(p.warnings[0].contains("npm.0"));
    }

    #[test]
    fn npm_optional_overrides_parse() {
        let p = parse(&lbl(&[
            ("docktornet.npm.0.host", "a"),
            ("docktornet.npm.0.port", "80"),
            ("docktornet.npm.0.scheme", "http"),
            ("docktornet.npm.0.ssl", "true"),
            ("docktornet.npm.0.hsts", "false"),
            ("docktornet.npm.0.hsts_subdomains", "false"),
            ("docktornet.npm.0.http2", "false"),
            ("docktornet.npm.0.block_exploits", "false"),
            ("docktornet.npm.0.websockets", "false"),
            ("docktornet.npm.0.cert_id", "42"),
        ]));
        assert_eq!(p.npm.len(), 1);
        assert_eq!(p.npm[0].hsts, Some(false));
        assert_eq!(p.npm[0].hsts_subdomains, Some(false));
        assert_eq!(p.npm[0].http2, Some(false));
        assert_eq!(p.npm[0].block_exploits, Some(false));
        assert_eq!(p.npm[0].websockets, Some(false));
        assert_eq!(p.npm[0].cert_id, Some(42));
    }

    #[test]
    fn parse_bool_is_case_insensitive() {
        let p = parse(&lbl(&[
            ("docktornet.npm.0.host", "a.example.com"),
            ("docktornet.npm.0.port", "3000"),
            ("docktornet.npm.0.scheme", "http"),
            ("docktornet.npm.0.ssl", "True"),
            ("docktornet.npm.0.hsts", "YES"),
            ("docktornet.npm.0.http2", "Off"),
        ]));
        assert_eq!(p.npm.len(), 1);
        assert!(p.npm[0].ssl);
        assert_eq!(p.npm[0].hsts, Some(true));
        assert_eq!(p.npm[0].http2, Some(false));
        assert!(p.warnings.is_empty());
    }

    #[test]
    fn npm_malformed_optional_field_degrades_not_drops() {
        let p = parse(&lbl(&[
            ("docktornet.npm.0.host", "a.example.com"),
            ("docktornet.npm.0.port", "3000"),
            ("docktornet.npm.0.scheme", "http"),
            ("docktornet.npm.0.ssl", "true"),
            // a single malformed optional field must not un-publish the host
            ("docktornet.npm.0.cert_id", "abc"),
            ("docktornet.npm.0.hsts", "maybe"),
        ]));
        assert_eq!(
            p.npm.len(),
            1,
            "host must survive a malformed optional field"
        );
        let h = &p.npm[0];
        assert_eq!(h.hosts, vec!["a.example.com".to_string()]);
        assert_eq!(h.port, 3000);
        assert!(h.ssl);
        assert_eq!(h.cert_id, None);
        assert_eq!(h.hsts, None);
        // the operator still gets a signal for each degraded field
        assert_eq!(p.warnings.len(), 2);
        assert!(p.warnings.iter().all(|w| w.contains("npm.0")));
    }

    #[test]
    fn npm_non_canonical_index_does_not_collide_with_canonical() {
        // "0" and "00" both parse to 0u8, which previously merged two distinct
        // label groups into a single NPM host, silently dropping a public
        // hostname. The canonical "0" host must survive; the non-canonical
        // "00" group must be rejected with a warning rather than overwriting it.
        let p = parse(&lbl(&[
            ("docktornet.npm.0.host", "a.example.com"),
            ("docktornet.npm.0.port", "3000"),
            ("docktornet.npm.0.scheme", "http"),
            ("docktornet.npm.0.ssl", "true"),
            ("docktornet.npm.00.host", "b.example.com"),
            ("docktornet.npm.00.port", "4000"),
            ("docktornet.npm.00.scheme", "http"),
            ("docktornet.npm.00.ssl", "true"),
        ]));
        assert_eq!(p.npm.len(), 1, "canonical host must not be merged away");
        assert_eq!(p.npm[0].hosts, vec!["a.example.com".to_string()]);
        assert_eq!(p.npm[0].port, 3000);
        assert!(
            p.warnings.iter().any(|w| w.contains("00")),
            "non-canonical index must produce a warning, got {:?}",
            p.warnings
        );
    }

    #[test]
    fn gatus_non_canonical_index_does_not_collide_with_canonical() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://a"),
            ("docktornet.gatus.0.interval", "30s"),
            ("docktornet.gatus.00.url", "https://b"),
            ("docktornet.gatus.00.interval", "60s"),
        ]));
        assert_eq!(
            p.gatus.len(),
            1,
            "canonical monitor must not be merged away"
        );
        assert_eq!(p.gatus[0].url, "https://a");
        assert!(
            p.warnings.iter().any(|w| w.contains("00")),
            "non-canonical index must produce a warning, got {:?}",
            p.warnings
        );
    }

    #[test]
    fn cf_dns_parses_when_all_fields_present() {
        let p = parse(&lbl(&[
            ("docktornet.cf.dns.name", "myapp"),
            ("docktornet.cf.dns.target", "home.example.com"),
            ("docktornet.cf.dns.proxied", "true"),
        ]));
        let dns = p.cf_dns.unwrap();
        assert_eq!(dns.name, "myapp");
        assert_eq!(dns.target, "home.example.com");
        assert!(dns.proxied);
    }

    #[test]
    fn cf_dns_partial_emits_warning() {
        let p = parse(&lbl(&[
            ("docktornet.cf.dns.name", "myapp"),
            // missing target
        ]));
        assert!(p.cf_dns.is_none());
        assert_eq!(p.warnings.len(), 1);
    }

    #[test]
    fn gatus_with_default_conditions() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
        ]));
        assert_eq!(p.gatus.len(), 1);
        assert_eq!(p.gatus[0].conditions, vec!["[STATUS] == 200"]);
    }

    #[test]
    fn gatus_explicit_conditions_split_on_comma() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
            (
                "docktornet.gatus.0.conditions",
                "[STATUS] == 200, [RESPONSE_TIME] < 500",
            ),
        ]));
        assert_eq!(
            p.gatus[0].conditions,
            vec![
                "[STATUS] == 200".to_string(),
                "[RESPONSE_TIME] < 500".to_string(),
            ]
        );
    }

    #[test]
    fn gatus_empty_explicit_conditions_use_default() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
            ("docktornet.gatus.0.conditions", " , ,, "),
        ]));
        assert_eq!(p.gatus.len(), 1);
        assert_eq!(p.gatus[0].conditions, vec!["[STATUS] == 200"]);
    }

    #[test]
    fn unknown_fields_under_known_integrations_warn_without_blocking_valid_fields() {
        let p = parse(&lbl(&[
            ("docktornet.npm.0.host", "a.example.com"),
            ("docktornet.npm.0.port", "3000"),
            ("docktornet.npm.0.scheme", "http"),
            ("docktornet.npm.0.ssl", "true"),
            ("docktornet.npm.0.hots", "typo"),
            ("docktornet.cf.dns.name", "myapp"),
            ("docktornet.cf.dns.target", "home.example.com"),
            ("docktornet.cf.dns.proxied", "true"),
            ("docktornet.cf.dns.proxxy", "typo"),
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
            ("docktornet.gatus.0.conditons", "typo"),
        ]));

        assert_eq!(p.npm.len(), 1);
        assert!(p.cf_dns.is_some());
        assert_eq!(p.gatus.len(), 1);
        assert_eq!(p.warnings.len(), 3);
        assert!(p.warnings.iter().any(|w| w.contains("npm.0.hots")));
        assert!(p.warnings.iter().any(|w| w.contains("cf.dns.proxxy")));
        assert!(p.warnings.iter().any(|w| w.contains("gatus.0.conditons")));
    }

    #[test]
    fn unrelated_docktornet_keys_become_warnings() {
        let p = parse(&lbl(&[("docktornet.unknown.0.foo", "bar")]));
        assert!(p.npm.is_empty());
        assert!(p.cf_dns.is_none());
        assert!(p.gatus.is_empty());
        assert_eq!(p.warnings.len(), 1);
        assert!(p.warnings[0].contains("docktornet.unknown"));
    }

    #[test]
    fn gatus_telegram_defaults_to_true_when_absent() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
        ]));
        assert_eq!(p.gatus.len(), 1);
        assert!(p.gatus[0].telegram);
    }

    #[test]
    fn gatus_group_defaults_to_none_when_absent() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
        ]));
        assert_eq!(p.gatus.len(), 1);
        assert_eq!(p.gatus[0].group, None);
    }

    #[test]
    fn gatus_empty_group_is_treated_as_none() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
            ("docktornet.gatus.0.group", ""),
        ]));
        assert_eq!(p.gatus[0].group, None);
    }

    #[test]
    fn gatus_telegram_false_is_respected() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
            ("docktornet.gatus.0.telegram", "false"),
        ]));
        assert_eq!(p.gatus.len(), 1);
        assert!(!p.gatus[0].telegram);
    }

    #[test]
    fn gatus_group_is_pass_through_when_set() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
            ("docktornet.gatus.0.group", "media"),
        ]));
        assert_eq!(p.gatus[0].group, Some("media".to_string()));
    }

    #[test]
    fn gatus_invalid_telegram_emits_warning_and_skips() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
            ("docktornet.gatus.0.telegram", "maybe"),
        ]));
        assert!(p.gatus.is_empty());
        assert_eq!(p.warnings.len(), 1);
        assert!(p.warnings[0].contains("gatus.0"));
    }

    #[test]
    fn gatus_name_parsed_when_set() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
            ("docktornet.gatus.0.name", "Homepage"),
        ]));
        assert_eq!(p.gatus[0].name, Some("Homepage".to_string()));
    }

    #[test]
    fn gatus_name_defaults_to_none_when_absent() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
        ]));
        assert_eq!(p.gatus[0].name, None);
    }

    #[test]
    fn gatus_empty_name_is_treated_as_none() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
            ("docktornet.gatus.0.name", ""),
        ]));
        assert_eq!(p.gatus[0].name, None);
    }

    #[test]
    fn gatus_severity_and_area_parse_when_set() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
            ("docktornet.gatus.0.severity", "critical"),
            ("docktornet.gatus.0.area", "network"),
        ]));
        assert!(p.warnings.is_empty(), "{:?}", p.warnings);
        assert_eq!(p.gatus[0].severity, Some(Severity::Critical));
        assert_eq!(p.gatus[0].area, Some(Area::Network));
    }

    #[test]
    fn gatus_severity_and_area_are_none_when_absent() {
        let p = parse(&lbl(&[
            ("docktornet.gatus.0.url", "https://x"),
            ("docktornet.gatus.0.interval", "30s"),
        ]));
        assert_eq!(p.gatus[0].severity, None);
        assert_eq!(p.gatus[0].area, None);
    }

    #[test]
    fn gatus_unknown_severity_or_area_drops_the_monitor_with_a_warning() {
        for (field, value) in [
            ("severity", "urgent"),
            ("severity", "Critical"),
            ("severity", ""),
            ("area", "databases"),
        ] {
            let key = format!("docktornet.gatus.0.{field}");
            let p = parse(&lbl(&[
                ("docktornet.gatus.0.url", "https://x"),
                ("docktornet.gatus.0.interval", "30s"),
                (key.as_str(), value),
            ]));
            assert!(p.gatus.is_empty(), "{field}={value:?} must not default");
            assert_eq!(p.warnings.len(), 1);
            assert!(p.warnings[0].contains("gatus.0"), "{:?}", p.warnings);
            assert!(p.warnings[0].contains(field), "{:?}", p.warnings);
        }
    }
}
