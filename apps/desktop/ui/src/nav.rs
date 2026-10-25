//! Navigation model.

/// Main navigation sections, in display order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Section {
    Home,
    Connections,
    Explorer,
    QueryStudio,
    SchemaStudio,
    Migrations,
    SyncJobs,
    Imports,
    Exports,
    Transformations,
    Schedules,
    Monitor,
    Reports,
    Logs,
    Settings,
}

impl Section {
    pub const ALL: [Section; 15] = [
        Self::Home,
        Self::Connections,
        Self::Explorer,
        Self::QueryStudio,
        Self::SchemaStudio,
        Self::Migrations,
        Self::SyncJobs,
        Self::Imports,
        Self::Exports,
        Self::Transformations,
        Self::Schedules,
        Self::Monitor,
        Self::Reports,
        Self::Logs,
        Self::Settings,
    ];

    pub fn label_key(&self) -> &'static str {
        match self {
            Self::Home => "nav.home",
            Self::Connections => "nav.connections",
            Self::Explorer => "nav.explorer",
            Self::QueryStudio => "nav.query_studio",
            Self::SchemaStudio => "nav.schema_studio",
            Self::Migrations => "nav.migrations",
            Self::SyncJobs => "nav.sync_jobs",
            Self::Imports => "nav.imports",
            Self::Exports => "nav.exports",
            Self::Transformations => "nav.transformations",
            Self::Schedules => "nav.schedules",
            Self::Monitor => "nav.monitor",
            Self::Reports => "nav.reports",
            Self::Logs => "nav.logs",
            Self::Settings => "nav.settings",
        }
    }

    /// Single-glyph icon. Glyphs avoid shipping an icon font.
    pub fn icon(&self) -> &'static str {
        match self {
            Self::Home => "⌂",
            Self::Connections => "⇄",
            Self::Explorer => "☰",
            Self::QueryStudio => "⌕",
            Self::SchemaStudio => "◫",
            Self::Migrations => "➜",
            Self::SyncJobs => "↻",
            Self::Imports => "⇣",
            Self::Exports => "⇡",
            Self::Transformations => "ƒ",
            Self::Schedules => "◷",
            Self::Monitor => "◉",
            Self::Reports => "▤",
            Self::Logs => "≡",
            Self::Settings => "⚙",
        }
    }

    /// Whether the section is functional in this build. Unavailable sections
    /// are shown disabled; they never open a placeholder screen.
    pub fn is_available(&self) -> bool {
        matches!(
            self,
            Self::Home
                | Self::Connections
                | Self::Explorer
                | Self::QueryStudio
                | Self::Logs
                | Self::Settings
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_section_has_unique_label() {
        let mut keys: Vec<_> = Section::ALL.iter().map(Section::label_key).collect();
        keys.sort_unstable();
        keys.dedup();
        assert_eq!(keys.len(), Section::ALL.len());
    }
}
