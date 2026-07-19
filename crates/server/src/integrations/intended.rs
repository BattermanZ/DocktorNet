use docktornet_core::{ContainerEntry, MachineId, labels::ParsedLabels, service_id};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntendedService {
    pub service_id: String,
    pub machine_id: MachineId,
    pub container: String,
    pub labels: ParsedLabels,
}

pub fn intended_services(
    machine_id: &MachineId,
    containers: &[ContainerEntry],
) -> Vec<IntendedService> {
    containers
        .iter()
        .filter_map(|container| {
            let parsed = docktornet_core::labels::parse(&container.labels);
            if !parsed.warnings.is_empty() {
                tracing::warn!(
                    service_id = %service_id(machine_id, &container.name),
                    container = %container.name,
                    warnings = ?parsed.warnings,
                    "dropping malformed docktornet labels; affected resources will not be published"
                );
            }
            let has_intent =
                !parsed.npm.is_empty() || parsed.cf_dns.is_some() || !parsed.gatus.is_empty();
            has_intent.then(|| IntendedService {
                service_id: service_id(machine_id, &container.name),
                machine_id: machine_id.clone(),
                container: container.name.clone(),
                labels: parsed,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn entry(name: &str, labels: &[(&str, &str)]) -> ContainerEntry {
        ContainerEntry {
            name: name.to_string(),
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<_, _>>(),
        }
    }

    #[test]
    fn ignores_containers_without_docktornet_labels() {
        let services = intended_services(
            &MachineId::new("m1"),
            &[entry("plain", &[("traefik.enable", "true")])],
        );
        assert!(services.is_empty());
    }

    #[derive(Clone)]
    struct BufWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for BufWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
        type Writer = BufWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn logs_warnings_for_malformed_labels() {
        let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(BufWriter(buf.clone()))
            .with_max_level(tracing::Level::WARN)
            .finish();

        // `port` is missing, so the npm host fails to build and is dropped.
        // Without surfacing warnings the public proxy host silently vanishes.
        tracing::subscriber::with_default(subscriber, || {
            let services = intended_services(
                &MachineId::new("m1"),
                &[entry(
                    "app",
                    &[("docktornet.npm.0.host", "app.example.com")],
                )],
            );
            assert!(services.is_empty());
        });

        let logs = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            logs.contains("npm.0"),
            "expected a warning mentioning the dropped npm.0 host, got: {logs:?}"
        );
    }

    #[test]
    fn parses_opted_in_container() {
        let services = intended_services(
            &MachineId::new("m1"),
            &[entry(
                "app",
                &[
                    ("docktornet.npm.0.host", "app.example.com"),
                    ("docktornet.npm.0.port", "3000"),
                    ("docktornet.npm.0.scheme", "http"),
                    ("docktornet.npm.0.ssl", "true"),
                ],
            )],
        );
        assert_eq!(services.len(), 1);
        assert_eq!(services[0].service_id, "m1/app");
        assert_eq!(services[0].labels.npm[0].hosts, vec!["app.example.com"]);
    }
}
