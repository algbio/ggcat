

pub struct Utils;

impl Utils {
    #[inline]
    pub fn pos_from_letter(letter: u8) -> u8 {
        match letter as char {
            'A' => 0,
            'C' => 1,
            'G' => 2,
            'T' => 3,
            'N' => 4,
            _ => panic!("Wrong letter {}", letter)
        }
    }
}