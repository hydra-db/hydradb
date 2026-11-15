//! Lexical numeric primitives shared by parser lowering and character casts.

/// Normalize Unicode Nd to ASCII without changing punctuation or separators
/// (§21.4 SR2). Adjacent Nd groups remain ordered groups of ten digits.
pub fn decimal_spelling(text: &str) -> String {
    use icu_properties::{props::GeneralCategory, CodePointMapData};
    let categories = CodePointMapData::<GeneralCategory>::new();
    text.chars()
        .map(|character| {
            if categories.get(character) != GeneralCategory::DecimalNumber {
                return character;
            }
            let code = character as u32;
            let mut start = code;
            while start > 0 && categories.get32(start - 1) == GeneralCategory::DecimalNumber {
                start -= 1;
            }
            char::from(b'0' + ((code - start) % 10) as u8)
        })
        .collect()
}

/// A nonempty digit run with optional single underscores between digits.
/// Decimal runs allow Unicode Nd; other radices retain their ASCII alphabet.
pub fn separated_digits(text: &str, radix: u32) -> Option<String> {
    if text.is_empty() || text.starts_with('_') || text.ends_with('_') || text.contains("__") {
        return None;
    }
    let spelling = if radix == 10 { decimal_spelling(text) } else { text.to_string() };
    let digits: String = spelling.chars().filter(|character| *character != '_').collect();
    digits.chars().all(|character| character.is_digit(radix)).then_some(digits)
}
