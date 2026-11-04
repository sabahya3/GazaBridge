//! `mongodb+srv://` seedlist discovery when the driver's resolver cannot start.
//!
//! The driver reads the system DNS settings, including search domains, and
//! refuses to resolve anything if one of them is not a valid DNS name (seen
//! on macOS when the search domain was set to the sentence "There aren't any
//! Search Domains set on Wi-Fi"). SRV and TXT names are absolute, so search
//! domains never matter for them. When the driver fails with a DNS error this
//! module performs the same lookups against the system's name servers, with
//! no search domains, applies the same checks as the driver (hosts must share
//! the parent domain, at most one TXT record, only the allowed TXT options),
//! and returns an equivalent `mongodb://` connection string with TLS on.

use std::net::IpAddr;

use hickory_resolver::TokioResolver;
use hickory_resolver::config::{NameServerConfig, ResolverConfig};
use hickory_resolver::proto::rr::{Name, RData};
use mongodb::error::ErrorKind;
use mongodb::options::ClientOptions;

const SCHEME: &str = "mongodb+srv://";

/// Parse a connection string, falling back to our own SRV resolution when
/// the driver fails during DNS resolution.
pub(crate) async fn parse(uri: &str) -> Result<ClientOptions, String> {
    match ClientOptions::parse(uri).await {
        Ok(o) => Ok(o),
        Err(e) if uri.starts_with(SCHEME) && matches!(*e.kind, ErrorKind::DnsResolve { .. }) => {
            match expand(uri).await {
                Ok(seedlist) => ClientOptions::parse(&seedlist)
                    .await
                    .map_err(|e| e.to_string()),
                Err(fallback) => Err(format!("{e} (direct SRV lookup also failed: {fallback})")),
            }
        }
        Err(e) => Err(e.to_string()),
    }
}

/// The parts of a `mongodb+srv://` string.
#[derive(Debug, PartialEq)]
struct SrvUri<'a> {
    userinfo: Option<&'a str>,
    host: &'a str,
    path: &'a str,
    options: Vec<(&'a str, &'a str)>,
}

fn split(uri: &str) -> Result<SrvUri<'_>, String> {
    let rest = uri
        .strip_prefix(SCHEME)
        .ok_or("not a mongodb+srv connection string")?;
    let end = rest.find(['/', '?']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(end);
    let (userinfo, host) = match authority.rsplit_once('@') {
        Some((u, h)) => (Some(u), h),
        None => (None, authority),
    };
    if host.is_empty() || host.contains([',', ':']) {
        return Err("mongodb+srv needs exactly one host name without a port".into());
    }
    let (path, query) = tail.split_once('?').unwrap_or((tail, ""));
    let options = query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| p.split_once('=').unwrap_or((p, "")))
        .collect();
    Ok(SrvUri {
        userinfo,
        host,
        path,
        options,
    })
}

/// SRV results must end with the queried host's parent domain (the whole
/// host when it has fewer than three labels), as the driver requires.
fn same_domain(original: &str, host: &str) -> bool {
    let original: Vec<_> = original.split('.').collect();
    let domain = if original.len() >= 3 {
        &original[1..]
    } else {
        &original[..]
    };
    let parts: Vec<_> = host.split('.').collect();
    parts.len() > 1 && parts[1..].ends_with(domain)
}

/// Options a TXT record may set; the connection string wins over them.
fn txt_options(txt: &str) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    for pair in txt.split('&').filter(|p| !p.is_empty()) {
        let Some((k, v)) = pair.split_once('=') else {
            return Err(format!("TXT option '{pair}' is not key=value"));
        };
        match k.to_ascii_lowercase().as_str() {
            "authsource" | "replicaset" | "loadbalanced" => out.push((k.into(), v.into())),
            _ => return Err(format!("TXT option '{k}' is not allowed with mongodb+srv")),
        }
    }
    Ok(out)
}

fn rebuild(
    srv: &SrvUri<'_>,
    mut hosts: Vec<(String, u16)>,
    txt: Vec<(String, String)>,
) -> Result<String, String> {
    let has = |key: &str| srv.options.iter().any(|(k, _)| k.eq_ignore_ascii_case(key));
    if let Some((_, max)) = srv
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("srvMaxHosts"))
    {
        let max: usize = max.parse().map_err(|_| "srvMaxHosts must be a number")?;
        if max > 0 {
            hosts.truncate(max);
        }
    }
    let mut options: Vec<String> = srv
        .options
        .iter()
        .filter(|(k, _)| {
            !k.eq_ignore_ascii_case("srvMaxHosts") && !k.eq_ignore_ascii_case("srvServiceName")
        })
        .map(|(k, v)| format!("{k}={v}"))
        .collect();
    for (k, v) in txt {
        if !has(&k) {
            options.push(format!("{k}={v}"));
        }
    }
    if !has("tls") && !has("ssl") {
        options.push("tls=true".into());
    }
    let hosts = hosts
        .iter()
        .map(|(h, p)| format!("{h}:{p}"))
        .collect::<Vec<_>>()
        .join(",");
    let userinfo = srv.userinfo.map(|u| format!("{u}@")).unwrap_or_default();
    let path = if srv.path.is_empty() { "/" } else { srv.path };
    Ok(format!(
        "mongodb://{userinfo}{hosts}{path}?{}",
        options.join("&")
    ))
}

/// Name servers from `/etc/resolv.conf`, ignoring its search line. macOS
/// keeps this file in step with the primary resolver.
fn system_name_servers() -> Result<Vec<NameServerConfig>, String> {
    let text = std::fs::read_to_string("/etc/resolv.conf")
        .map_err(|e| format!("cannot read the system name servers: {e}"))?;
    let servers: Vec<_> = text
        .lines()
        .filter_map(|l| l.trim().strip_prefix("nameserver"))
        .filter_map(|ip| ip.trim().parse::<IpAddr>().ok())
        .map(NameServerConfig::udp_and_tcp)
        .collect();
    if servers.is_empty() {
        return Err("no name servers found in /etc/resolv.conf".into());
    }
    Ok(servers)
}

async fn expand(uri: &str) -> Result<String, String> {
    let srv = split(uri)?;
    let service = srv
        .options
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("srvServiceName"))
        .map_or("mongodb", |(_, v)| v);
    let resolver = TokioResolver::builder_with_config(
        ResolverConfig::from_name_servers(system_name_servers()?),
        Default::default(),
    )
    .build()
    .map_err(|e| e.to_string())?;
    let absolute = |s: String| Name::from_str_relaxed(format!("{s}.")).map_err(|e| e.to_string());

    let lookup = resolver
        .srv_lookup(absolute(format!("_{service}._tcp.{}", srv.host))?)
        .await
        .map_err(|e| e.to_string())?;
    let mut hosts = Vec::new();
    for record in lookup.answers() {
        let RData::SRV(r) = &record.data else {
            continue;
        };
        let host = r.target.to_utf8().trim_end_matches('.').to_string();
        if !same_domain(srv.host, &host) {
            return Err(format!(
                "SRV result {host} is outside the domain of {}",
                srv.host
            ));
        }
        hosts.push((host, r.port));
    }
    if hosts.is_empty() {
        return Err(format!("no SRV records for {}", srv.host));
    }

    let txt = match resolver.txt_lookup(absolute(srv.host.to_string())?).await {
        Ok(lookup) => {
            let records: Vec<String> = lookup
                .answers()
                .iter()
                .filter_map(|r| match &r.data {
                    RData::TXT(t) => Some(
                        t.txt_data
                            .iter()
                            .map(|b| String::from_utf8_lossy(b).into_owned())
                            .collect(),
                    ),
                    _ => None,
                })
                .collect();
            if records.len() > 1 {
                return Err(format!("more than one TXT record for {}", srv.host));
            }
            records
                .first()
                .map(|t| txt_options(t))
                .transpose()?
                .unwrap_or_default()
        }
        Err(e) if e.is_no_records_found() => Vec::new(),
        Err(e) => return Err(e.to_string()),
    };
    rebuild(&srv, hosts, txt)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_a_connection_string() {
        let s = split("mongodb+srv://u:p%40x@cluster0.example.net/app?appName=C0&retryWrites=true")
            .unwrap();
        assert_eq!(s.userinfo, Some("u:p%40x"));
        assert_eq!(s.host, "cluster0.example.net");
        assert_eq!(s.path, "/app");
        assert_eq!(s.options, vec![("appName", "C0"), ("retryWrites", "true")]);
        let bare = split("mongodb+srv://cluster0.example.net").unwrap();
        assert_eq!((bare.userinfo, bare.path), (None, ""));
        assert!(split("mongodb+srv://a.example.net:27017/").is_err());
        assert!(split("mongodb+srv://a.example.net,b.example.net/").is_err());
    }

    #[test]
    fn checks_the_domain_like_the_driver() {
        assert!(same_domain(
            "cluster0.abc.mongodb.net",
            "ac-1-shard-00-00.abc.mongodb.net"
        ));
        assert!(!same_domain("cluster0.abc.mongodb.net", "evil.example.com"));
        assert!(!same_domain("cluster0.abc.mongodb.net", "x.mongodb.net"));
        assert!(same_domain("example.com", "db.example.com"));
    }

    #[test]
    fn rebuilds_a_seedlist_string() {
        let s = split("mongodb+srv://u@c.abc.net/app?authSource=admin&srvMaxHosts=2").unwrap();
        let hosts = vec![
            ("a.abc.net".to_string(), 27017),
            ("b.abc.net".to_string(), 27017),
            ("c.abc.net".to_string(), 27017),
        ];
        let txt = txt_options("authSource=other&replicaSet=rs0").unwrap();
        assert_eq!(
            rebuild(&s, hosts, txt).unwrap(),
            "mongodb://u@a.abc.net:27017,b.abc.net:27017/app?authSource=admin&replicaSet=rs0&tls=true"
        );
        let plain = split("mongodb+srv://c.abc.net?tls=false").unwrap();
        assert_eq!(
            rebuild(&plain, vec![("a.abc.net".into(), 1)], vec![]).unwrap(),
            "mongodb://a.abc.net:1/?tls=false"
        );
    }

    #[test]
    fn refuses_unknown_txt_options() {
        assert!(txt_options("tls=false").is_err());
        assert!(txt_options("authSource").is_err());
    }

    /// Resolves a real SRV name through the fallback (DNS only, no
    /// connection): `GAZABRIDGE_SRV_HOST=cluster.example.net`.
    #[tokio::test]
    #[ignore = "needs network and GAZABRIDGE_SRV_HOST"]
    async fn expands_a_real_srv_name() {
        let Ok(host) = std::env::var("GAZABRIDGE_SRV_HOST") else {
            return;
        };
        let uri = expand(&format!("mongodb+srv://{host}/")).await.unwrap();
        assert!(
            uri.starts_with("mongodb://") && uri.contains("tls=true"),
            "{uri}"
        );
        ClientOptions::parse(&uri).await.unwrap();
        let opts = parse(&format!("mongodb+srv://{host}/")).await.unwrap();
        assert!(!opts.hosts.is_empty());
        let named = parse(&format!("mongodb+srv://u@{host}/app")).await.unwrap();
        eprintln!(
            "authentication database with a user: {:?}",
            named.credential.and_then(|c| c.source)
        );
        if let Err(e) = ClientOptions::parse(format!("mongodb+srv://{host}/")).await {
            eprintln!("the driver alone fails here: {e}");
        }
    }
}
