//! Metadata-only actor n0; data voters stay n1/n2/n3 throughout retirement.
//! Initial placement is declared before any native birth, never edited online.
use super::*;
use crate::db::FreshRangeGenesisAuthority;
use crate::distribution::{ClusterNode, RangeReplica, RangeReplicaRole, ReplicaId};
use crate::distribution_data::ClusterDataNodeService;
use crate::{DistributionConfig, DistributionStore};

pub(super) struct ActorFixture {
    pub(super) root: tempfile::TempDir,
    pub(super) topology: Arc<RwLock<ClusterTopology>>,
    pub(super) databases: BTreeMap<ClusterNodeId, Arc<RwLock<BicDb>>>,
    pub(super) fresh_factories: BTreeMap<ClusterNodeId, FreshRangeGenesisAuthority>,
    pub(super) services: BTreeMap<ClusterNodeId, Arc<Mutex<ClusterDataNodeService>>>,
}
impl ActorFixture {
    pub(super) fn new(network: &BTreeMap<ClusterNodeId, (String, String)>) -> Self {
        let root = tempfile::tempdir().unwrap();
        let cluster = ClusterId::new("actual-native-metadata-actor").unwrap();
        let bootstrap = Uuid::now_v7();
        let mut distribution = DistributionStore::initialize_at(
            root.path().join("initial-placement"),
            DistributionConfig {
                enabled: true,
                cluster_id: cluster.clone(),
                node_id: node(1),
                node_address: network[&node(1)].0.clone(),
                initial_ranges: 1,
                replication_factor: 3,
                range_image_bootstrap_id: Some(bootstrap),
                ..Default::default()
            },
            true,
            1,
        )
        .unwrap();
        for i in [0, 2, 3] {
            distribution
                .join_node(
                    ClusterNode::new(node(i), network[&node(i)].0.clone(), 1, 1_000_000, 1)
                        .unwrap(),
                    &node(1),
                    1,
                )
                .unwrap();
        }
        let mut topology = distribution.topology().clone();
        // Genuine initial configuration, before creation of any database or
        // consensus store. Only 0/1/2 vote in metadata; n0 has no data replica.
        topology.generation = 1;
        topology.history.truncate(1);
        topology.nodes.get_mut(&node(3)).unwrap().metadata_role = MetadataMemberRole::Learner;
        for range in topology.ranges.values_mut() {
            assert_eq!(range.leader, node(1));
            assert_eq!(range.replicas.len(), 1);
            for i in [2, 3] {
                range.replicas.push(RangeReplica {
                    id: ReplicaId::new(topology.next_replica_id).unwrap(),
                    node_id: node(i),
                    role: RangeReplicaRole::Voter,
                });
                topology.next_replica_id += 1;
            }
        }
        for (id, member) in &mut topology.nodes {
            member.tls_certificate_sha256 = Some(network[id].1.clone());
        }
        topology.validate().unwrap();
        let members = topology
            .nodes
            .iter()
            .map(|(id, member)| {
                (
                    id.clone(),
                    RangeImageBirthMember {
                        incarnation: member.incarnation,
                        tls_certificate_sha256: member.tls_certificate_sha256.clone(),
                        pending_tls_certificate_sha256: member
                            .pending_tls_certificate_sha256
                            .clone(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut databases = BTreeMap::new();
        let mut fresh_factories = BTreeMap::new();
        let mut enrollments = Vec::new();
        let mut schema = None;
        for (id, member) in &topology.nodes {
            let spec = RangeImageBirthSpec {
                cluster_id: cluster.clone(),
                bootstrap_id: bootstrap,
                node_id: id.clone(),
                member_incarnation: member.incarnation,
                initial_members: members.clone(),
                restore_activation: None,
                initial_ranges: topology
                    .ranges
                    .values()
                    .filter(|range| range.replicas.iter().any(|replica| replica.node_id == *id))
                    .cloned()
                    .collect(),
            };
            let path = root.path().join(id.as_str());
            let (mut db, _) =
                BicDb::create_new_enrolled_range_image_database(&path, config(), spec.clone())
                    .unwrap();
            db.create_collection("items").unwrap();
            drop(db);
            let (db, factory) = BicDb::open_range_image_database(&path, config(), &spec).unwrap();
            let fingerprint = db.schema_compatibility_fingerprint().unwrap().sha256;
            if let Some(expected) = &schema {
                assert_eq!(expected, &fingerprint);
            } else {
                schema = Some(fingerprint);
            }
            let db = Arc::new(RwLock::new(db));
            let gate = db.read().range_mutation_gate();
            for range in &spec.initial_ranges {
                let frozen = gate.try_freeze(range.id, Uuid::now_v7()).unwrap();
                BicDb::initialize_range_image_genesis(
                    &db,
                    &factory,
                    range,
                    &frozen,
                    Duration::from_secs(3),
                    &CancellationToken::uncancelable(),
                )
                .unwrap();
            }
            enrollments.push(crate::distribution_native_admission::ActualNativeStoreEnrollment::from_actual_factory(
                &factory, &db.read(), &topology,
            ).unwrap());
            fresh_factories.insert(id.clone(), factory);
            databases.insert(id.clone(), db);
        }
        for enrollment in enrollments {
            enrollment
                .bind_initial_or_unassigned_learner(&mut topology)
                .unwrap();
        }
        for member in topology.nodes.values_mut() {
            member.labels.insert(
                crate::distribution::SCHEMA_COMPATIBILITY_NODE_LABEL.into(),
                schema.clone().unwrap(),
            );
        }
        let services = databases
            .iter()
            .map(|(id, db)| {
                let service = ClusterDataNodeService::open(
                    cluster.clone(),
                    id.clone(),
                    db.read().data_path(),
                    Arc::clone(db),
                    true,
                )
                .unwrap();
                (id.clone(), Arc::new(Mutex::new(service)))
            })
            .collect();
        Self {
            root,
            topology: Arc::new(RwLock::new(topology)),
            databases,
            fresh_factories,
            services,
        }
    }
}
