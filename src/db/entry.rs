#[derive(Debug, Clone)]
pub struct Entry {
    pub ts: u64,
    pub value: f64,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.ts == other.ts && (other.value - self.value).abs() <= 1e-6
    }
}

#[test]
fn test_eq() {
    assert_eq!(Entry { ts: 1, value: 1.0 }, Entry { ts: 1, value: 1.0 });
}
