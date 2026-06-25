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
