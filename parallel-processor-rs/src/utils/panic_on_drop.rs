pub struct PanicOnDrop(&'static str);

impl PanicOnDrop {
    pub fn new(message: &'static str) -> Self {
        Self(message)
    }

    pub fn disengage(self) {
        std::mem::forget(self);
    }
}

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic!("Cannot drop value: {}", self.0);
    }
}
