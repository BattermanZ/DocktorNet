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
    pub host: String,
    pub port: u16,
    pub scheme: NpmScheme,
    pub ssl: bool,
    pub hsts: Option<bool>,
    pub http2: Option<bool>,
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
            ["npm", idx, field] => match idx.parse::<u8>() {
                Ok(index) => {
                    npm_groups
                        .entry(index)
                        .or_default()
                        .insert((*field).to_string(), value.clone());
                }
                Err(_) => out
                    .warnings
                    .push(format!("invalid npm index in key '{key}'")),
            },
            ["gatus", idx, field] => match idx.parse::<u8>() {
                Ok(index) => {
                    gatus_groups
                        .entry(index)
                        .or_default()
                        .insert((*field).to_string(), value.clone());
                }
                Err(_) => out
                    .warnings
                    .push(format!("invalid gatus index in key '{key}'")),
            },
            ["cf", "dns", field] => {
                cf_dns.insert((*field).to_string(), value.clone());
            }
            _ => out
                .warnings
                .push(format!("unrecognised docktornet key '{key}'")),
        }
    }

    for (index, fields) in npm_groups {
        match build_npm_host(index, &fields) {
            Ok(host) => out.npm.push(host),
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

fn build_npm_host(index: u8, fields: &BTreeMap<String, String>) -> Result<NpmHost, String> {
    let host = fields.get("host").ok_or("missing host")?.clone();
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
    let hsts = fields.get("hsts").map(parse_bool).transpose()?;
    let http2 = fields.get("http2").map(parse_bool).transpose()?;
    let cert_id = fields
        .get("cert_id")
        .map(|cert_id| cert_id.parse::<u32>())
        .transpose()
        .map_err(|_| "cert_id must be u32".to_string())?;

    Ok(NpmHost {
        index,
        host,
        port,
        scheme,
        ssl,
        hsts,
        http2,
        cert_id,
    })
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

fn build_gatus(index: u8, fields: &BTreeMap<String, String>) -> Result<GatusMonitor, String> {
    let url = fields.get("url").ok_or("missing url")?.clone();
    let interval = fields.get("interval").ok_or("missing interval")?.clone();
    let conditions = match fields.get("conditions") {
        Some(conditions) => conditions
            .split(',')
            .map(|condition| condition.trim().to_string())
            .filter(|condition| !condition.is_empty())
            .collect(),
        None => vec!["[STATUS] == 200".to_string()],
    };

    Ok(GatusMonitor {
        index,
        url,
        interval,
        conditions,
    })
}

fn parse_bool(value: impl AsRef<str>) -> Result<bool, String> {
    match value.as_ref() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        other => Err(format!("expected bool, got '{other}'")),
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
        assert_eq!(h.host, "a.example.com");
        assert_eq!(h.port, 3000);
        assert_eq!(h.scheme, NpmScheme::Http);
        assert!(h.ssl);
        assert_eq!(h.hsts, None);
        assert_eq!(h.cert_id, None);
        assert!(p.warnings.is_empty());
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
            ("docktornet.npm.0.http2", "false"),
            ("docktornet.npm.0.cert_id", "42"),
        ]));
        assert_eq!(p.npm.len(), 1);
        assert_eq!(p.npm[0].hsts, Some(false));
        assert_eq!(p.npm[0].http2, Some(false));
        assert_eq!(p.npm[0].cert_id, Some(42));
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
    fn unrelated_docktornet_keys_become_warnings() {
        let p = parse(&lbl(&[("docktornet.unknown.0.foo", "bar")]));
        assert!(p.npm.is_empty());
        assert!(p.cf_dns.is_none());
        assert!(p.gatus.is_empty());
        assert_eq!(p.warnings.len(), 1);
        assert!(p.warnings[0].contains("docktornet.unknown"));
    }
}
