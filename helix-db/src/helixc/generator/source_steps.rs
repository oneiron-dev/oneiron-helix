use core::fmt;
use std::fmt::Display;

use crate::helixc::generator::utils::{
    VecData, write_properties, write_properties_slice, write_secondary_indices,
};

use super::{
    bool_ops::BoExp,
    utils::{GenRef, GeneratedValue},
};

fn format_pre_filter(pre_filter: &Option<Vec<BoExp>>) -> String {
    match pre_filter {
        Some(filters) => {
            let formatted = filters
                .iter()
                .map(|f| format!("|v: &HVector, txn: &RoTxn| {f}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("Some(&[{}])", formatted)
        }
        None => "None".to_string(),
    }
}

#[derive(Clone, Debug)]
pub enum SourceStep {
    /// Traversal starts from an identifier
    Identifier(GenRef<String>),
    /// Add a node
    AddN(AddN),
    /// Add an edge
    AddE(AddE),
    /// Insert a vector
    AddV(AddV),
    /// Lookup a node by ID
    NFromID(NFromID),
    /// Lookup a node by index
    NFromIndex(NFromIndex),
    /// Lookup a node by type
    NFromType(NFromType),
    /// Lookup an edge by ID
    EFromID(EFromID),
    /// Lookup an edge by type
    EFromType(EFromType),
    /// Lookup a vector by ID
    VFromID(VFromID),
    /// Lookup a vector by type
    VFromType(VFromType),
    /// Search for vectors
    SearchVector(SearchVector),
    /// Search for vectors using BM25
    SearchBM25(SearchBM25),
    /// Hybrid search combining vector and BM25 with RRF fusion
    SearchHybrid(SearchHybrid),
    /// Personalized PageRank
    PPR(PPR),
    Upsert(Upsert),
    /// Traversal starts from an anonymous node
    Anonymous,
    Empty,
}

#[derive(Clone, Debug)]
pub struct Upsert {
    /// Properties of node
    pub properties: Option<Vec<(String, GeneratedValue)>>,

    /// Names of properties to index on
    pub secondary_indices: Option<Vec<String>>,
}

#[derive(Clone, Debug)]
pub struct AddN {
    /// Label of node
    pub label: GenRef<String>,
    /// Properties of node
    pub properties: Option<Vec<(String, GeneratedValue)>>,
    /// Names of properties to index on
    pub secondary_indices: Option<Vec<String>>,
}
impl Display for AddN {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let properties = write_properties(&self.properties);
        let secondary_indices = write_secondary_indices(&self.secondary_indices);
        write!(
            f,
            "add_n({}, {}, {})",
            self.label, properties, secondary_indices
        )
    }
}

#[derive(Clone, Debug)]
pub struct AddE {
    /// Label of edge
    pub label: GenRef<String>,
    /// Properties of edge
    pub properties: Option<Vec<(String, GeneratedValue)>>,
    /// From node ID
    pub from: GeneratedValue,
    /// To node ID
    pub to: GeneratedValue,
    /// Whether from is a plural variable (needs iteration)
    pub from_is_plural: bool,
    /// Whether to is a plural variable (needs iteration)
    pub to_is_plural: bool,
    pub is_unique: bool,
}
impl Display for AddE {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let props = write_properties(&self.properties);
        match (self.from_is_plural, self.to_is_plural) {
            (false, false) => {
                write!(
                    f,
                    "add_edge({}, {}, {}, {}, false, {})",
                    self.label, props, self.from, self.to, self.is_unique
                )
            }
            (true, false) => {
                write!(
                    f,
                    "{{
    let mut edge = Vec::new();
    for from_val in {}.iter() {{
        let e = G::new_mut(&db, &arena, &mut txn)
            .add_edge({}, {}, from_val.id(), {}, false, {})
            .collect_to_obj()?;
        edge.push(e);
    }}
    edge
}}",
                    self.from, self.label, props, self.to, self.is_unique
                )
            }
            (false, true) => {
                write!(
                    f,
                    "{{
    let mut edge = Vec::new();
    for to_val in {}.iter() {{
        let e = G::new_mut(&db, &arena, &mut txn)
            .add_edge({}, {}, {}, to_val.id(), false, {})
            .collect_to_obj()?;
        edge.push(e);
    }}
    edge
}}",
                    self.to, self.label, props, self.from, self.is_unique
                )
            }
            (true, true) => {
                write!(
                    f,
                    "{{
    let mut edge = Vec::new();
    for from_val in {}.iter() {{
        for to_val in {}.iter() {{
            let e = G::new_mut(&db, &arena, &mut txn)
                .add_edge({}, {}, from_val.id(), to_val.id(), false, {})
                .collect_to_obj()?;
            edge.push(e);
        }}
    }}
    edge
}}",
                    self.from, self.to, self.label, props, self.is_unique
                )
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct UpsertN {
    /// Label of node
    pub label: GenRef<String>,
    /// Properties of node
    pub properties: Option<Vec<(String, GeneratedValue)>>,
    /// Names of properties to index on
    pub secondary_indices: Option<Vec<String>>,
}

impl Display for UpsertN {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let props = self.properties.as_ref().map_or_else(
            || write_properties_slice(&Some(Vec::new())),
            |_| write_properties_slice(&self.properties),
        );
        write!(f, "upsert_n({}, {})", self.label, props)
    }
}

#[derive(Clone, Debug)]
pub struct UpsertE {
    /// Label of edge
    pub label: GenRef<String>,
    /// Properties of edge
    pub properties: Option<Vec<(String, GeneratedValue)>>,
    /// From node ID
    pub from: GeneratedValue,
    /// To node ID
    pub to: GeneratedValue,
    /// Whether from is a plural variable (needs iteration)
    pub from_is_plural: bool,
    /// Whether to is a plural variable (needs iteration)
    pub to_is_plural: bool,
}
impl Display for UpsertE {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let props = self.properties.as_ref().map_or_else(
            || write_properties_slice(&Some(Vec::new())),
            |_| write_properties_slice(&self.properties),
        );
        match (self.from_is_plural, self.to_is_plural) {
            (false, false) => {
                write!(f, "upsert_e({}, {}, {}, {})", self.label, self.from, self.to, props)
            }
            (true, false) => {
                write!(
                    f,
                    "{}.iter().map(|from_val| {{
        G::new_mut(&db, &arena, &mut txn)
        .upsert_e({}, from_val.id(), {}, {})
        .collect_to_obj()
    }}).collect::<Result<Vec<_>,_>>()?",
                    self.from, self.label, self.to, props
                )
            }
            (false, true) => {
                write!(
                    f,
                    "{}.iter().map(|to_val| {{
        G::new_mut(&db, &arena, &mut txn)
        .upsert_e({}, {}, to_val.id(), {})
        .collect_to_obj()
    }}).collect::<Result<Vec<_>,_>>()?",
                    self.to, self.label, self.from, props
                )
            }
            (true, true) => {
                write!(
                    f,
                    "{}.iter().flat_map(|from_val| {{
        {}.iter().map(move |to_val| {{
            G::new_mut(&db, &arena, &mut txn)
            .upsert_e({}, from_val.id(), to_val.id(), {})
            .collect_to_obj()
        }})
    }}).collect::<Result<Vec<_>,_>>()?",
                    self.from, self.to, self.label, props
                )
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct UpsertV {
    /// Vector to upsert
    pub vec: VecData,
    /// Label of vector
    pub label: GenRef<String>,
    /// Properties of vector
    pub properties: Option<Vec<(String, GeneratedValue)>>,
}
impl Display for UpsertV {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let props = self.properties.as_ref().map_or_else(
            || write_properties_slice(&Some(Vec::new())),
            |_| write_properties_slice(&self.properties),
        );
        write!(f, "upsert_v({}, {}, {})", self.vec, self.label, props)
    }
}

#[derive(Clone, Debug)]
pub struct AddV {
    /// Vector to add
    pub vec: VecData,
    /// Label of vector
    pub label: GenRef<String>,
    /// Properties of vector
    pub properties: Option<Vec<(String, GeneratedValue)>>,
}
impl Display for AddV {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "insert_v::<fn(&HVector, &RoTxn) -> bool>({}, {}, {})",
            self.vec,
            self.label,
            write_properties(&self.properties)
        )
    }
}

#[derive(Clone, Debug)]
pub struct NFromID {
    /// ID of node
    pub id: GenRef<String>,
    /// Label of node
    ///
    /// - unused currently but kept in the case ID lookups need to be from specific table based on type
    pub label: GenRef<String>,
}
impl Display for NFromID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n_from_id({})", self.id)
    }
}

#[derive(Clone, Debug)]
pub struct NFromType {
    /// Label of nodes to lookup
    pub label: GenRef<String>,
}
impl Display for NFromType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n_from_type({})", self.label)
    }
}

#[derive(Clone, Debug)]
pub struct EFromID {
    /// ID of edge
    pub id: GenRef<String>,
    /// Label of edge
    ///
    /// - unused currently but kept in the case ID lookups need to be from specific table based on type
    pub label: GenRef<String>,
}
impl Display for EFromID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "e_from_id({})", self.id)
    }
}

#[derive(Clone, Debug)]
pub struct EFromType {
    /// Label of edges to lookup
    pub label: GenRef<String>,
}
impl Display for EFromType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "e_from_type({})", self.label)
    }
}

#[derive(Clone, Debug)]
pub struct VFromID {
    /// ID of vector
    pub id: GenRef<String>,
    /// Label of vector
    pub label: GenRef<String>,

    /// Whether to get the vector data
    pub get_vector_data: bool,
}

impl Display for VFromID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v_from_id({}, {})", self.id, self.get_vector_data)
    }
}

#[derive(Clone, Debug)]
pub struct VFromType {
    /// Label of vectors to lookup
    pub label: GenRef<String>,
    /// Whether to get the vector data
    pub get_vector_data: bool,
}

impl Display for VFromType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v_from_type({}, {})", self.label, self.get_vector_data)
    }
}

#[derive(Clone, Debug)]
pub struct SearchBM25 {
    /// Type of node to search for
    pub type_arg: GenRef<String>,
    /// Query to search for
    pub query: GeneratedValue,
    /// Number of results to return
    pub k: GeneratedValue,
}

impl Display for SearchBM25 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "search_bm25({}, {}, {})?",
            self.type_arg, self.query, self.k
        )
    }
}

impl Display for SourceStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SourceStep::Identifier(_) => write!(f, ""),
            SourceStep::AddN(add_n) => write!(f, "{add_n}"),
            SourceStep::AddE(add_e) => write!(f, "{add_e}"),
            SourceStep::AddV(add_v) => write!(f, "{add_v}"),
            SourceStep::NFromID(n_from_id) => write!(f, "{n_from_id}"),
            SourceStep::NFromIndex(n_from_index) => write!(f, "{n_from_index}"),
            SourceStep::NFromType(n_from_type) => write!(f, "{n_from_type}"),
            SourceStep::EFromID(e_from_id) => write!(f, "{e_from_id}"),
            SourceStep::EFromType(e_from_type) => write!(f, "{e_from_type}"),
            SourceStep::SearchVector(search_vector) => write!(f, "{search_vector}"),
            SourceStep::SearchBM25(search_bm25) => write!(f, "{search_bm25}"),
            SourceStep::SearchHybrid(search_hybrid) => write!(f, "{search_hybrid}"),
            SourceStep::PPR(ppr) => write!(f, "{ppr}"),
            SourceStep::Upsert(upsert) => write!(f, "upsert({:?})", upsert),
            SourceStep::Anonymous => write!(f, ""),
            SourceStep::Empty => {
                debug_assert!(false, "SourceStep::Empty should not reach generator");
                write!(f, "/* ERROR: empty source step */")
            }
            SourceStep::VFromID(v_from_id) => write!(f, "{v_from_id}"),
            SourceStep::VFromType(v_from_type) => write!(f, "{v_from_type}"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SearchVector {
    /// Label of vector to search for
    pub label: GenRef<String>,
    /// Vector to search for
    pub vec: VecData,
    /// Number of results to return
    pub k: GeneratedValue,
    /// Pre-filter to apply to the search - currently not implemented in grammar
    pub pre_filter: Option<Vec<BoExp>>,
}

impl Display for SearchVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "search_v::<fn(&HVector, &RoTxn) -> bool, _>({}, {}, {}, {})",
            self.vec, self.k, self.label, format_pre_filter(&self.pre_filter)
        )
    }
}

#[derive(Clone, Debug)]
pub struct SearchHybrid {
    /// Label of vector type to search
    pub label: GenRef<String>,
    /// Vector data to search for
    pub vec: VecData,
    /// Text query for BM25 search
    pub text_query: GeneratedValue,
    /// Number of results to return
    pub k: GeneratedValue,
    /// Pre-filter to apply to vector search
    pub pre_filter: Option<Vec<BoExp>>,
}

impl Display for SearchHybrid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{
    let __hybrid_vec_results: Vec<_> = G::new(&db, &txn, &arena)
        .search_v::<fn(&HVector, &RoTxn) -> bool, _>({}, {}, {}, {})
        .collect::<Result<Vec<_>, _>>()?;
    let __hybrid_bm25_results: Vec<_> = G::new(&db, &txn, &arena)
        .search_bm25({}, &{}, {})?
        .collect::<Result<Vec<_>, _>>()?;
    RRFReranker::fuse_lists(
        vec![__hybrid_vec_results.into_iter(), __hybrid_bm25_results.into_iter()],
        60.0
    ).map_err(|e| GraphError::from(e.to_string()))?
}}",
            self.vec, self.k, self.label, format_pre_filter(&self.pre_filter),
            self.label, self.text_query, self.k
        )
    }
}

#[derive(Clone, Debug)]
pub struct PPR {
    pub node_type: GenRef<String>,
    pub seeds: GenRef<String>,
    pub universe: GenRef<String>,
    pub depth: Option<GeneratedValue>,
    pub damping: Option<GeneratedValue>,
    pub limit: GeneratedValue,
}

impl Display for PPR {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let depth = self.depth.as_ref().map_or("3".to_string(), |d| format!("{d}"));
        let damping = self.damping.as_ref().map_or("0.85".to_string(), |d| format!("{d}"));
        write!(
            f,
            "{{
    use helix_db::helix_engine::storage_core::storage_methods::StorageMethods;
    let ppr_universe: std::collections::HashSet<u128> = {}.iter().map(|id| **id).collect();
    let ppr_seeds: Vec<u128> = {}.iter().map(|id| **id).collect();
    let ppr_results = helix_db::helix_engine::graph::ppr::ppr_with_storage(
        &db,
        &txn,
        &arena,
        &ppr_universe,
        &ppr_seeds,
        &std::collections::HashMap::new(),
        {} as usize,
        {},
        {} as usize,
        true,
    );
    ppr_results.into_iter().filter_map(|(node_id, _score)| {{
        db.get_node(&txn, &node_id, &arena).ok().map(TraversalValue::Node)
    }}).collect::<Vec<_>>()
}}",
            self.universe, self.seeds, depth, damping, self.limit
        )
    }
}

#[derive(Clone, Debug)]
pub struct NFromIndex {
    /// Index to search against
    pub index: GenRef<String>,
    /// Key to search for in the index
    pub key: GeneratedValue,
    /// Label of nodes to lookup - used for post filtering
    pub label: GenRef<String>,
}

impl Display for NFromIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "n_from_index({}, {}, {})",
            self.label, self.index, self.key
        )
    }
}
