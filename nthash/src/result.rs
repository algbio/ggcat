use super::MAXIMUM_K_SIZE;

error_chain! {
    errors {
        KSizeOutOfRange(ksize: usize, seq_size: usize) {
            description("K size is out of range for the give sequence")
            display("K size {} is out of range for the given sequence size {}", ksize, seq_size)
        }
        KSizeTooBig(ksize: usize) {
            description("K size cannnot exceed the size of a u32")
            display("K size {} cannot exceed the size of a u32 {}", ksize, MAXIMUM_K_SIZE)
        }
    }
}
