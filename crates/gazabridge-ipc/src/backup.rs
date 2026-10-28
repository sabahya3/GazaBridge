//! Backups and restores.

use serde::{Deserialize, Serialize};

use crate::explorer::ObjectAddr;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDto {
    /// `mongodump`, `mongorestore`, `pg_dump`, `pg_restore`.
    pub name: String,
    pub found: bool,
    pub path: Option<String>,
    pub version: Option<String>,
    pub install_hint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupArgs {
    pub op_id: String,
    pub connection_id: String,
    pub objects: Vec<ObjectAddr>,
    /// `gazabridge` (default) or `native`.
    #[serde(default)]
    pub method: String,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupObjectDto {
    pub object: ObjectAddr,
    pub records: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupDto {
    pub path: String,
    pub label: String,
    /// RFC 3339.
    pub created_at: String,
    /// `gazabridge` or `native`.
    pub method: String,
    /// `mongodb` or `postgres`.
    pub family: String,
    pub connection: String,
    pub target: String,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub objects: Vec<BackupObjectDto>,
    pub records: u64,
    pub bytes: u64,
    pub tool: Option<String>,
    /// Native backups: the command as run, without secrets.
    pub command: Option<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreArgs {
    pub op_id: String,
    pub backup: String,
    pub connection_id: String,
    #[serde(default)]
    pub rename_to: Option<String>,
    #[serde(default)]
    pub only: Vec<ObjectAddr>,
    /// `skip` (default), `update`, `fail` or `replace`.
    #[serde(default)]
    pub existing: String,
    #[serde(default)]
    pub confirm_destructive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreItemDto {
    pub from: ObjectAddr,
    pub to: ObjectAddr,
    pub records: u64,
    pub target_exists: bool,
    pub target_count: Option<u64>,
    pub verified: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestorePlanDto {
    pub items: Vec<RestoreItemDto>,
    pub statements: Vec<String>,
    pub command: Option<String>,
    pub blockers: Vec<String>,
    pub warnings: Vec<String>,
    pub destructive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupStatusDto {
    /// `running`, `done`, `failed`, `cancelled`.
    pub phase: String,
    pub step: String,
    pub objects_done: usize,
    pub objects_total: usize,
    pub records: u64,
    pub log: Vec<String>,
    pub error: Option<String>,
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathArgs {
    pub path: String,
}
