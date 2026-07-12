use serde::Deserialize;
use serde_yaml::Value;

fn documents(source: &str) -> Vec<Value> {
    serde_yaml::Deserializer::from_str(source)
        .map(|document| Value::deserialize(document).expect("valid deployment YAML"))
        .collect()
}

fn resource<'a>(documents: &'a [Value], kind: &str, name: &str) -> &'a Value {
    documents
        .iter()
        .find(|document| {
            document["kind"].as_str() == Some(kind)
                && document["metadata"]["name"].as_str() == Some(name)
        })
        .unwrap_or_else(|| panic!("missing {kind} {name}"))
}

fn sequence_contains_port(value: &Value, port: u64) -> bool {
    value.as_sequence().is_some_and(|rules| {
        rules.iter().any(|rule| {
            rule["ports"].as_sequence().is_some_and(|ports| {
                ports
                    .iter()
                    .any(|entry| entry["port"].as_u64() == Some(port))
            })
        })
    })
}

fn internal_tls_volume_projects_ca(stateful_set: &Value) -> bool {
    stateful_set["spec"]["template"]["spec"]["volumes"]
        .as_sequence()
        .and_then(|volumes| {
            volumes
                .iter()
                .find(|volume| volume["name"].as_str() == Some("internal-tls"))
        })
        .and_then(|volume| volume["projected"]["sources"].as_sequence())
        .is_some_and(|sources| {
            sources.iter().any(|source| {
                source["secret"]["name"].as_str() == Some("graph-internal-ca")
                    && source["secret"]["items"]
                        .as_sequence()
                        .is_some_and(|items| {
                            items.iter().any(|item| {
                                item["key"].as_str() == Some("tls.crt")
                                    && item["path"].as_str() == Some("ca.crt")
                            })
                        })
            })
        })
}

#[test]
fn production_network_allows_node_to_controller_rpc() {
    let manifests = documents(include_str!("../deploy/base/network-policy.yaml"));
    let policy = resource(&manifests, "NetworkPolicy", "graph-runtime-traffic");
    assert!(sequence_contains_port(&policy["spec"]["egress"], 9443));
}

#[test]
fn internal_mtls_projects_a_deterministic_ca_bundle() {
    let certificates = documents(include_str!(
        "../deploy/addons/cert-manager/certificate.yaml"
    ));
    let ca = resource(&certificates, "Certificate", "graph-internal-ca");
    assert_eq!(ca["spec"]["secretName"].as_str(), Some("graph-internal-ca"));
    assert_eq!(ca["spec"]["isCA"].as_bool(), Some(true));
    assert_eq!(
        ca["spec"]["privateKey"]["rotationPolicy"].as_str(),
        Some("Never")
    );
    resource(&certificates, "Issuer", "graph-internal-ca-issuer");

    for source in [
        include_str!("../deploy/base/controller-statefulset.yaml"),
        include_str!("../deploy/base/node-statefulset.yaml"),
    ] {
        let manifests = documents(source);
        let stateful_set = manifests
            .iter()
            .find(|document| document["kind"].as_str() == Some("StatefulSet"))
            .expect("stateful set manifest");
        assert!(internal_tls_volume_projects_ca(stateful_set));
    }
}

#[test]
fn eks_overlay_includes_production_secret_provisioners() {
    let manifests = documents(include_str!("../deploy/overlays/eks/kustomization.yaml"));
    let resources = manifests[0]["resources"]
        .as_sequence()
        .expect("EKS resources");
    for required in ["../../addons/cert-manager", "../../addons/external-secrets"] {
        assert!(
            resources
                .iter()
                .any(|resource| resource.as_str() == Some(required)),
            "EKS overlay is missing {required}"
        );
    }
}
