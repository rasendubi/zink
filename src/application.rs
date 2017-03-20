use std::collections::HashMap;

pub trait Extension {
    fn handle_request(&self, path: &[&str], payload: &[u8]);
}

pub struct Application<'a> {
    extensions: HashMap<&'a str, Box<Extension + Sync + Send>>,
}

impl<'a> Application<'a> {
    pub fn new() -> Application<'a> {
        Application {
            extensions: HashMap::new(),
        }
    }

    pub fn register_extension(&mut self, extension_name: &'a str, extension: Box<Extension + Sync + Send>) {
        self.extensions.insert(extension_name, extension);
    }

    pub fn handle_request(&self, path: &[&str], payload: &[u8]) {
        if path.is_empty() {
            return;
        }

        if let Some(extension) = self.extensions.get(path[0]) {
            extension.handle_request(&path[1..], payload);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestExtension<'a> {
        pub expected_path: &'a [&'a str],
        pub expected_payload: &'a [u8],
        pub times_called: AtomicUsize,
    }

    impl<'a> TestExtension<'a> {
        pub fn new() -> TestExtension<'a> {
            TestExtension {
                expected_path: &[],
                expected_payload: &[],
                times_called: AtomicUsize::new(0),
            }
        }
    }

    impl<'a> Extension for TestExtension<'a> {
        fn handle_request(&self, path: &[&str], payload: &[u8]) {
            assert_eq!(self.expected_path, path);
            assert_eq!(self.expected_payload, payload);
            self.times_called.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn create_empty_application() {
        let _ = Application::new();
    }

    #[test]
    fn register_extension() {
        let test_extension = Box::new(TestExtension::new());

        let mut app = Application::new();
        app.register_extension("test", test_extension);
    }

    #[test]
    fn handle_request() {
        let test_extension = TestExtension::new();

        {
            let mut app = Application::new();
            app.register_extension("test", Box::new(test_extension));
            app.handle_request(&["test"], &[]);
        }

        // assert_eq!(1, test_extension.times_called.load(Ordering::Relaxed));
    }
}
