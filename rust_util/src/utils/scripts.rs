// Scripts

use logic::utils::scripts;
use logic::utils::scripts::parse_nginx_log::ServerStatistic;

use cpython::{py_fn, PyDict, PyList, PyModule, PyResult, Python, PythonObject};

// Python Module definition
pub fn module(py: Python<'_>) -> PyResult<PyModule> {
    let scripts = PyModule::new(py, "scripts")?;
    scripts.add(py, "__doc__", "Util - Scripts")?;

    scripts.add(
        py,
        "parse_nginx_log",
        py_fn!(py, parse_nginx_log(path: &str)),
    )?;

    Ok(scripts)
}

fn server_statistic_to_dict(py: Python<'_>, statistic: ServerStatistic) -> PyResult<PyDict> {
    let dict = PyDict::new(py);

    dict.set_item(py, "access_timestamp", statistic.access_timestamp)?;
    dict.set_item(py, "path", statistic.path)?;

    match &statistic.http_method {
        Some(http_method) => dict.set_item(py, "http_method", http_method)?,
        None => dict.set_item(py, "http_method", py.None())?,
    }

    dict.set_item(py, "count", statistic.count)?;
    dict.set_item(py, "kb_sent", statistic.kb_sent)?;
    dict.set_item(py, "total_duration", statistic.total_duration)?;
    dict.set_item(py, "min_duration", statistic.min_duration)?;
    dict.set_item(py, "max_duration", statistic.max_duration)?;
    dict.set_item(py, "errors", statistic.errors)?;

    Ok(dict)
}

fn parse_nginx_log(py: Python<'_>, path: &str) -> PyResult<PyList> {
    if let Ok(results) = scripts::parse_nginx_log::parse_nginx_log(path) {
        let list = PyList::new(py, &[]);

        for (i, result) in results.into_iter().enumerate() {
            let dict = server_statistic_to_dict(py, result)?;
            list.insert(py, i, dict.into_object());
        }

        Ok(list)
    } else {
        Ok(PyList::new(py, &[]))
    }
}
