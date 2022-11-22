use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use serde_json::Deserializer;
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MLData {
    pub nodes: Vec<Node>,
    pub tree: Vec<TreeNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Node {
    pub i: String,
    #[serde(default = "default_fnz_id")]
    fnz_id: String,
    pub a: HashMap<String, String>,
}

fn default_fnz_id() -> String {
    String::from("-1")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TreeNode {
    pub i: String,
    pub c: Option<Vec<TreeNode>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MLDataContainer {
    element_statistics: MLData,
}

fn read_ml_json(json_str: &str) -> MLDataContainer{
    let mut deserializer = serde_json::Deserializer::from_str(&json_str);
    deserializer.disable_recursion_limit();
    let deserializer = serde_stacker::Deserializer::new(&mut deserializer);

    MLDataContainer::deserialize(deserializer).unwrap()
}

#[pyfunction]
pub fn correlacion_json(json_str: &str, json_str_current: &str
) -> PyResult<Vec<f64>> {
    let data = read_ml_json(json_str);
    let current = read_ml_json(json_str_current);
    let xx_opt = data.element_statistics.nodes.iter().filter(
        |&n|n.a.get("XX").is_some()).next();
    
    let ignore = HashSet::from(["HT","LT","TP","WH"]);

    if let Some(xx) = xx_opt {
        return Ok(current.element_statistics.nodes.iter().map(|n|{
            let fields:Vec<&str> =  n.a.iter().map(|(s,v)| &s[..]).filter(|s|!ignore.contains(*s)).filter(|s| xx.a.contains_key(*s)).collect();
            fields.iter().filter(|&&s| xx.a.get(s).unwrap() == n.a.get(s).unwrap()).count() as f64 / (xx.a.len()-5) as f64   
        }).collect());
    }
    Ok(Vec::new())
}

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

/// A Python module implemented in Rust.
#[pymodule]
fn rust_python(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(correlacion_json, m)?)?;
    Ok(())
}