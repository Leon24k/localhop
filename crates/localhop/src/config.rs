use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct LocalhopConfig {
    pub server: Option<String>,
    pub token: Option<String>,
    pub inspector_port: Option<u16>,
    #[serde(default)]
    pub no_inspector: bool,
    #[serde(default)]
    pub quiet: bool,
    #[serde(default)]
    pub tunnels: Vec<TunnelSpec>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TunnelSpec {
    pub port: u16,
    #[serde(default = "default_host")]
    pub host: String,
    pub id: Option<String>,
    pub basic_auth: Option<String>,
    #[serde(default)]
    pub preserve_host: bool,
    #[serde(default)]
    pub headers: Vec<String>,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

pub fn load_config(path: &Path) -> Result<LocalhopConfig, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read config {}: {e}", path.display()))?;
    let cfg: LocalhopConfig =
        toml::from_str(&raw).map_err(|e| format!("invalid TOML in {}: {e}", path.display()))?;
    if cfg.tunnels.is_empty() {
        return Err(format!(
            "config {} must define at least one [[tunnels]] entry",
            path.display()
        ));
    }
    Ok(cfg)
}

pub fn parse_header_arg(raw: &str) -> Result<(String, String), String> {
    let Some((k, v)) = raw.split_once(':') else {
        return Err(format!(
            "invalid header '{raw}': expected 'Name: Value' format"
        ));
    };
    let name = k.trim();
    if name.is_empty() {
        return Err(format!(
            "invalid header '{raw}': header name cannot be empty"
        ));
    }
    Ok((name.to_string(), v.trim().to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_custom_headers() {
        assert_eq!(
            parse_header_arg("X-Env: staging").unwrap(),
            ("X-Env".into(), "staging".into())
        );
        assert_eq!(
            parse_header_arg("Authorization: Bearer abc:123").unwrap(),
            ("Authorization".into(), "Bearer abc:123".into())
        );
        assert!(parse_header_arg("NoColon").is_err());
        assert!(parse_header_arg(": empty-name").is_err());
    }

    #[test]
    fn parses_multi_tunnel_toml() {
        let toml_str = r#"
            server = "https://relay.example.com"
            inspector_port = 4041

            [[tunnels]]
            port = 3000
            id = "web"

            [[tunnels]]
            port = 8080
            id = "api"
            headers = ["X-Env: preview"]
        "#;
        let cfg: LocalhopConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.tunnels.len(), 2);
        assert_eq!(cfg.tunnels[0].host, "127.0.0.1");
        assert_eq!(cfg.tunnels[1].headers, vec!["X-Env: preview"]);
    }
}
