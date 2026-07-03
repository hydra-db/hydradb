//! Display and formatting utilities.

/// Format a number for display with SI suffixes and appropriate precision.
///
/// Large numbers are displayed with K/M/B suffixes, while smaller numbers
/// use appropriate decimal places based on magnitude.
///
/// # Examples
///
/// ```
/// use common::display::format_number;
///
/// assert_eq!(format_number(1234567.0), "1.23M");
/// assert_eq!(format_number(1500.0), "1.50K");
/// assert_eq!(format_number(42.0), "42");
/// assert_eq!(format_number(0.5), "0.50");
/// assert_eq!(format_number(0.005), "0.0050");
/// ```
pub fn format_number(value: f64) -> String {
    let abs = value.abs();

    // Determine appropriate precision based on magnitude
    let (formatted, suffix) = if abs >= 1_000_000_000.0 {
        (value / 1_000_000_000.0, "B")
    } else if abs >= 1_000_000.0 {
        (value / 1_000_000.0, "M")
    } else if abs >= 1_000.0 {
        (value / 1_000.0, "K")
    } else {
        (value, "")
    };

    // Format with appropriate decimal places
    if suffix.is_empty() {
        if abs == 0.0 {
            "0".to_string()
        } else if abs < 0.01 {
            format!("{:.4}", formatted)
        } else if abs < 1.0 {
            format!("{:.2}", formatted)
        } else if formatted.fract() == 0.0 {
            format!("{:.0}", formatted)
        } else {
            format!("{:.2}", formatted)
        }
    } else {
        format!("{:.2}{}", formatted, suffix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_format_billions() {
        assert_eq!(format_number(1_234_567_890.0), "1.23B");
        assert_eq!(format_number(5_000_000_000.0), "5.00B");
    }

    #[test]
    fn should_format_millions() {
        assert_eq!(format_number(1_234_567.0), "1.23M");
        assert_eq!(format_number(5_500_000.0), "5.50M");
    }

    #[test]
    fn should_format_thousands() {
        assert_eq!(format_number(1_234.0), "1.23K");
        assert_eq!(format_number(52_647.0), "52.65K");
    }

    #[test]
    fn should_format_whole_numbers() {
        assert_eq!(format_number(42.0), "42");
        assert_eq!(format_number(100.0), "100");
    }

    #[test]
    fn should_format_decimals() {
        assert_eq!(format_number(42.5), "42.50");
        assert_eq!(format_number(0.75), "0.75");
    }

    #[test]
    fn should_format_small_decimals() {
        assert_eq!(format_number(0.005), "0.0050");
        assert_eq!(format_number(0.0001), "0.0001");
    }

    #[test]
    fn should_handle_zero() {
        assert_eq!(format_number(0.0), "0");
    }

    #[test]
    fn should_handle_negative_numbers() {
        assert_eq!(format_number(-1_500.0), "-1.50K");
        assert_eq!(format_number(-42.5), "-42.50");
    }
}
