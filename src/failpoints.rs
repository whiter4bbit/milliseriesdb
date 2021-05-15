use std::sync::{Arc, Mutex};
use std::collections::HashMap;

#[derive(PartialEq)]
enum Action {
    Off,
    On,
}

pub struct Failpoints {
    actions: Arc<Mutex<HashMap<String, Action>>>,
}

impl Failpoints {
    pub fn create() -> Failpoints {
        Failpoints {
            actions: Arc::new(Mutex::new(HashMap::new())),
        }  
    }

    fn action<S: AsRef<str>>(&self, name: S, action: Action) {
        let mut actions = self.actions.lock().unwrap();
        actions.insert(name.as_ref().to_owned(), action);
    }
    
    pub fn on<S: AsRef<str>>(&self, name: S) {
        self.action(name, Action::On);
    }

    pub fn off<S: AsRef<str>>(&self, name: S) {
        self.action(name, Action::Off);
    }

    pub fn is_on<S: AsRef<str>>(&self, name: S) -> bool {
        let actions = self.actions.lock().unwrap();
        actions.get(name.as_ref()).unwrap_or(&Action::Off) == &Action::On
    }
}

#[cfg(test)]
#[macro_export]
macro_rules! failpoint {
    ($fp:expr, $name:expr, $ret:expr) => {
        if $fp.is_on($name) {
            return $ret;
        }
    }
}

#[cfg(not(test))]
#[macro_export]
macro_rules! failpoint {
    ($fp:expr, $name:expr, $ret:expr) => {
        {};
    }
}

pub use failpoint;

#[cfg(test)]
mod test {
    use super::*;

    fn write(fp: &Failpoints) -> Result<(), String>{
        failpoint!(fp, "write::error", Err("err".to_string()));

        Ok(())        
    }

    #[test]
    fn test_failpoints() {
        let fp = Failpoints::create();
        
        assert!(matches!(write(&fp), Ok(())));
        
        fp.on("write::error");
        write(&fp).unwrap_err();
        
        fp.off("write::error");
        assert!(matches!(write(&fp), Ok(())));
    }
}