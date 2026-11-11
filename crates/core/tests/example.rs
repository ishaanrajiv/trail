//! The example project's declarations must always be valid: the docs and the clients' tests use them.

#[test]
fn the_example_declarations_are_valid() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/shop/trail");
    let decl = match trail_core::decl::load_dir(&dir) {
        Ok(decl) => decl,
        Err(problems) => panic!(
            "{}",
            problems
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("\n")
        ),
    };
    assert_eq!(decl.source, "shop");
    assert_eq!(decl.events.len(), 3);
    assert_eq!(decl.metrics.len(), 2);
    assert_eq!(decl.rollups.len(), 2);
    assert_eq!(decl.alerts.len(), 3);
}
