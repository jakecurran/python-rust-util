//! Rust Util

use cpython::py_module_initializer;

mod utils;

use utils::scripts::module as scripts_module;

py_module_initializer!(rust_util, |py, m| {
    m.add(py, "__doc__", "Rust Util")?;

    // Modules
    m.add(py, "scripts", scripts_module(py)?)?;

    Ok(())
});
