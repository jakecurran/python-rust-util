//! parse_nginx_log

use std::collections::HashMap;
use std::error::Error;

use csv;
use hyper::Method as HttpMethod;
use rayon::prelude::*;
use regex::{Regex, RegexBuilder};

use serde;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// CONSTANTS

// Skips any requests with more than this number of resources in path
const MAX_PATH_DEPTH: usize = 15;

// Estimated sizes for allocation
const ROOT_PATHS_INIT_SIZE: usize = 25;
const FULL_PATHS_INIT_SIZE: usize = 1000;

// Delimiters
const LOG_DELIMITER: char = '|';
const RESOURCE_DELIMITER: char = '/';

// Param replacements
const INT_PARAM: &str = "<INT>";
const UUID_PARAM: &str = "<UUID>";
const CONTAINS_DIGIT_PARAM: &str = "<CONTAINS_DIGIT>";

#[derive(Debug)]
pub struct ServerStatistic {
    /// Request timestamp in format "YYYY-MM-DD HH:MM:SS"
    pub access_timestamp: String,
    /// HTTP request method
    pub http_method: Option<String>,
    /// Full request path
    pub path: String,
    /// Total number of requests
    pub count: u32,
    /// Total amount of data sent in kilobytes
    pub kb_sent: f32,
    /// Total duration in milliseconds
    pub total_duration: f32,
    /// Maximum duration in milliseconds
    pub min_duration: f32,
    /// Maximum duration in milliseconds
    pub max_duration: f32,
    // Total number of errors
    pub errors: u32,
}

// Parse into HashMap, then create tree
pub fn parse_nginx_log(path: &str) -> Result<Vec<ServerStatistic>, Box<dyn Error>> {
    let mut results = ParseNginxLog::default();
    let mut request_count: u32 = 0;

    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .delimiter(LOG_DELIMITER as u8)
        .from_path(path)?;

    let mut raw_record = csv::StringRecord::new();
    while csv_reader.read_record(&mut raw_record)? {
        match raw_record.deserialize(None) {
            Ok(request) => {
                results.insert(request);
                request_count += 1;
            }
            Err(e) => println!("{:?}", e),
        }
    }

    println!("Processed {} requests", request_count);

    results.build_hierarchies();

    let final_results = results.flatten();
    println!("Produced {} final result entries", final_results.len());

    Ok(final_results)
}

/// Struct that NGINX logs get deserialed into
#[derive(Debug, Deserialize)]
struct Request<'a> {
    // time in the ISO 8601 standard format
    #[serde(deserialize_with = "strip_minutes")]
    time_iso8601: String,

    // in this order of precedence:
    //   host name from the request line, or host name from the 'Host' request
    //   header field, or the server name matching a request
    #[allow(dead_code)]
    host: &'a str,
    // client address (unused, so no need for UTF-8 validation)
    #[allow(dead_code)]
    remote_addr: &'a [u8],
    // response status
    status: u16,
    // the number of bytes sent to a client
    bytes_sent: u32,
    // request processing time in seconds with a milliseconds resolution
    request_time: f32,

    // HTTP method and path
    #[serde(deserialize_with = "deserialize_request_path")]
    request: RequestPath,
}

#[derive(Debug, Default)]
struct RequestPath {
    http_method: HttpMethod,
    path: String,
}

/// Top level results struct
#[derive(Default, Debug)]
struct ParseNginxLog {
    hours: HashMap<String, ParseNginxLogHour>,
}

impl ParseNginxLog {
    fn insert(&mut self, request: Request<'_>) {
        self.hours
            .entry(request.time_iso8601.to_string())
            .or_insert_with(ParseNginxLogHour::default)
            .full_paths
            .get_or_insert(HashMap::new())
            .entry(request.request.path.to_string())
            .and_modify(|e| e.increment(&request))
            .or_insert_with(|| RequestNode::default_with_request(request));
    }

    fn build_hierarchies(&mut self) {
        self.hours
            .par_iter_mut()
            .for_each(|(k, v)| v.build_hierarchy(k));
    }

    fn flatten(&mut self) -> Vec<ServerStatistic> {
        let mut list: Vec<ServerStatistic> =
            Vec::with_capacity(self.hours.len() * FULL_PATHS_INIT_SIZE);

        self.hours.par_iter_mut().for_each(|(_, v)| v.flatten());
        self.hours
            .iter_mut()
            .for_each(|(_, v)| list.append(&mut v.flat));

        list
    }
}

/// Results for a specific hour
#[derive(Debug)]
struct ParseNginxLogHour {
    full_paths: Option<HashMap<String, RequestNode>>,
    hierarchy: Option<RequestNode>,
    flat: Vec<ServerStatistic>,
}

impl Default for ParseNginxLogHour {
    fn default() -> Self {
        ParseNginxLogHour {
            full_paths: Some(HashMap::with_capacity(FULL_PATHS_INIT_SIZE)),
            hierarchy: None,
            flat: Vec::new(),
        }
    }
}

impl ParseNginxLogHour {
    /// Builds RequestNode structure from full paths
    fn build_hierarchy(&mut self, time: &str) {
        self.hierarchy = Some(RequestNode::default());

        if let Some(hierarchy) = &mut self.hierarchy {
            hierarchy.access_timestamp = time.to_string();

            if let Some(full_paths) = &self.full_paths {
                for request in full_paths.values() {
                    hierarchy.insert(request);
                }
            }
        }

        self.full_paths = None;
    }

    /// Collects nodes into specified vector
    fn flatten(&mut self) {
        let mut vec = Vec::new();

        if let Some(hierarchy) = &self.hierarchy {
            hierarchy.flatten(&mut vec);
        }

        self.hierarchy = None;
        self.flat = vec;
    }
}

/// All metrics for a specific path
#[derive(Debug, Clone, Serialize)]
struct RequestNode {
    total: Metrics,

    #[serde(serialize_with = "serialize_methods")]
    http_methods: Option<HashMap<HttpMethod, Metrics>>,

    access_timestamp: String,
    path: String,
    children: HashMap<String, Box<RequestNode>>,
}

fn serialize_methods<S>(
    http_methods: &Option<HashMap<HttpMethod, Metrics>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match http_methods {
        Some(methods) => {
            let mut map = serializer.serialize_map(Some(methods.len()))?;
            for (k, v) in methods {
                map.serialize_entry(&k.to_string(), v)?;
            }

            map.end()
        }
        None => serializer.serialize_none(),
    }
}

/// Multiuse metrics struct
#[derive(Debug, Clone, Serialize)]
struct Metrics {
    count: u32,
    download: u32, // In bytes
    duration: f32, // In ms
    min_duration: Option<f32>,
    max_duration: Option<f32>,
    errors: u32,
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics {
            count: 0,
            download: 0,
            duration: 0.0,
            min_duration: None,
            max_duration: None,
            errors: 0,
        }
    }
}

impl Metrics {
    fn default_with_request(request: &Request) -> Self {
        let mut metrics = Metrics::default();
        metrics.increment(request);
        metrics
    }

    fn increment(&mut self, request: &Request) {
        self.count += 1;
        self.download += request.bytes_sent;

        match self.min_duration {
            Some(min_duration) => {
                if request.request_time < min_duration {
                    self.min_duration = Some(request.request_time);
                }
            }
            None => self.min_duration = Some(request.request_time),
        }

        match self.max_duration {
            Some(max_duration) => {
                if request.request_time > max_duration {
                    self.max_duration = Some(request.request_time);
                }
            }
            None => self.max_duration = Some(request.request_time),
        }

        self.duration += request.request_time;

        if request.status >= 500 {
            self.errors += 1;
        }
    }

    fn increment_with_metric(&mut self, metrics: &Metrics) {
        self.count += metrics.count;
        self.download += metrics.download;

        if metrics.min_duration < self.min_duration {
            self.min_duration = metrics.min_duration;
        }
        if metrics.max_duration > self.max_duration {
            self.max_duration = metrics.max_duration;
        }
        self.duration += metrics.duration;

        self.errors += metrics.errors;
    }
}

impl Default for RequestNode {
    fn default() -> Self {
        RequestNode {
            total: Metrics::default(),
            http_methods: None,

            access_timestamp: String::new(),
            path: RESOURCE_DELIMITER.to_string(),
            children: HashMap::with_capacity(ROOT_PATHS_INIT_SIZE),
        }
    }
}

impl RequestNode {
    fn default_with_request(request: Request<'_>) -> Self {
        let mut errors = 0;
        if request.status >= 500 {
            errors += 1;
        }

        let mut methods = HashMap::new();
        let method_metrics = Metrics::default_with_request(&request);
        methods.insert(request.request.http_method, method_metrics);

        RequestNode {
            total: Metrics {
                count: 1,
                download: request.bytes_sent,
                min_duration: Some(request.request_time),
                max_duration: Some(request.request_time),
                duration: request.request_time,
                errors,
            },
            http_methods: Some(methods),

            access_timestamp: request.time_iso8601,
            path: request.request.path,
            children: HashMap::with_capacity(ROOT_PATHS_INIT_SIZE),
        }
    }

    fn insert(&mut self, request: &RequestNode) {
        let resources: Vec<_> = request.path.split(RESOURCE_DELIMITER).collect();

        match resources.len() {
            0..=1 => return,
            2 => {
                self.increment_node(request);

                if let Some(methods) = &request.http_methods {
                    for (method, metrics) in methods {
                        self.http_methods
                            .get_or_insert_with(HashMap::new)
                            .entry(method.clone())
                            .and_modify(|e| e.increment_with_metric(metrics))
                            .or_insert_with(|| metrics.clone());
                    }
                }

                return;
            }
            3..=MAX_PATH_DEPTH => {}
            _ => return,
        }

        self.increment_node(request);

        let mut current_path = String::with_capacity(request.path.len());

        let mut current = self;
        for resource in resources.iter().skip(1) {
            current_path += &(RESOURCE_DELIMITER.to_string() + resource);

            current = current
                .children
                .entry(resource.to_string())
                .and_modify(|e| e.increment_node(request))
                .or_insert_with(|| {
                    Box::new(RequestNode {
                        total: request.total.clone(),
                        http_methods: None,

                        access_timestamp: request.access_timestamp.clone(),
                        path: current_path.clone(),
                        children: request.children.clone(),
                    })
                });
        }

        // only now increment methods if any
        if let Some(methods) = &request.http_methods {
            for (method, metrics) in methods {
                current
                    .http_methods
                    .get_or_insert_with(HashMap::new)
                    .entry(method.clone())
                    .and_modify(|e| e.increment_with_metric(metrics))
                    .or_insert_with(|| metrics.clone());
            }
        }
    }

    fn increment(&mut self, request: &Request) {
        self.total.count += 1;
        self.total.download += request.bytes_sent;

        match self.total.min_duration {
            Some(min_duration) => {
                if request.request_time < min_duration {
                    self.total.min_duration = Some(request.request_time);
                }
            }
            None => self.total.min_duration = Some(request.request_time),
        }

        match self.total.max_duration {
            Some(max_duration) => {
                if request.request_time > max_duration {
                    self.total.max_duration = Some(request.request_time);
                }
            }
            None => self.total.max_duration = Some(request.request_time),
        }

        self.total.duration += request.request_time;

        if request.status >= 500 {
            self.total.errors += 1;
        }

        self.http_methods
            .get_or_insert_with(HashMap::new)
            .entry(request.request.http_method.clone())
            .and_modify(|m| m.increment(request))
            .or_insert_with(|| Metrics::default_with_request(request));
    }

    fn increment_node(&mut self, request_node: &RequestNode) {
        self.total.count += request_node.total.count;
        self.total.download += request_node.total.download as u32;

        if request_node.total.min_duration < self.total.min_duration {
            self.total.min_duration = request_node.total.min_duration;
        }

        if request_node.total.max_duration > self.total.max_duration {
            self.total.max_duration = request_node.total.max_duration;
        }

        self.total.duration += request_node.total.duration;
        self.total.errors += request_node.total.errors;
    }

    fn flatten(&self, vec: &mut Vec<ServerStatistic>) {
        if let (Some(min_duration), Some(max_duration)) =
            (self.total.min_duration, self.total.max_duration)
        {
            vec.push(ServerStatistic {
                access_timestamp: self.access_timestamp.clone(),
                http_method: None,
                path: self.path.to_string(),
                count: self.total.count,
                kb_sent: self.total.download as f32 / 1024.0,
                total_duration: self.total.duration,
                min_duration,
                max_duration,
                errors: self.total.errors,
            });
        }

        if let Some(methods) = &self.http_methods {
            for (method, metrics) in methods {
                if let (Some(min_duration), Some(max_duration)) =
                    (metrics.min_duration, metrics.max_duration)
                {
                    vec.push(ServerStatistic {
                        access_timestamp: self.access_timestamp.clone(),
                        http_method: Some(method.to_string()),
                        path: self.path.to_string(),
                        count: metrics.count,
                        kb_sent: metrics.download as f32 / 1024.0,
                        total_duration: metrics.duration,
                        min_duration,
                        max_duration,
                        errors: metrics.errors,
                    });
                }
            }
        }

        for child in self.children.values() {
            child.flatten(vec);
        }
    }
}

/// Strip minutes from ISO 8601 date
fn strip_minutes<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;

    if s.len() < 14 {
        Ok(s.to_string())
    } else {
        Ok((&s[0..10]).to_string() + " " + &s[11..14] + "00")
    }
}

/// Deserialize request path
fn deserialize_request_path<'de, D>(deserializer: D) -> Result<RequestPath, D::Error>
where
    D: Deserializer<'de>,
{
    lazy_static! {
        static ref REQUEST: Regex =
            Regex::new(r#"^(GET|HEAD|POST|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH) (/[^\s\?#]*)"#)
                .unwrap();
        static ref UUID: Regex = RegexBuilder::new(
            r#"^[0-9A-F]{8}\-[0-9A-F]{4}\-[0-9A-F]{4}\-[0-9A-F]{4}\-[0-9A-F]{12}$"#
        )
        .case_insensitive(true)
        .build()
        .unwrap();
        static ref CONTAINS_DIGIT: Regex = Regex::new(r#"^.*[0-9]+.*$"#).unwrap();
    }

    let s: &str = Deserialize::deserialize(deserializer)?;
    if let Some(captures) = REQUEST.captures(s) {
        if captures.len() < 3 {
            return Ok(RequestPath::default());
        }

        if let Ok(http_method) = captures[1].parse::<HttpMethod>() {
            let path = captures[2].to_string();

            let resources: Vec<_> = path.split(RESOURCE_DELIMITER).collect();
            let mut resources_new = Vec::with_capacity(resources.len());

            for mut resource in resources {
                if resource.parse::<u32>().is_ok() {
                    resource = INT_PARAM;
                } else if UUID.is_match(resource) {
                    resource = UUID_PARAM;
                } else if CONTAINS_DIGIT.is_match(resource) {
                    resource = CONTAINS_DIGIT_PARAM;
                }

                resources_new.push(resource.to_string());
            }

            if resources_new[resources_new.len() - 1].is_empty() {
                resources_new.remove(resources_new.len() - 1);
            }

            return Ok(RequestPath {
                http_method,
                path: resources_new.join(&RESOURCE_DELIMITER.to_string()),
            });
        }
    }

    Ok(RequestPath::default())
}
