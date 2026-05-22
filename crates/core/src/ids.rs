// will be filled in Task 2

pub type MachineId = String;

pub fn service_id(machine_id: &MachineId, service_name: &str) -> String {
    sanitize_service_id(&format!("{machine_id}-{service_name}"))
}

pub fn sanitize_service_id(value: &str) -> String {
    value.to_owned()
}
