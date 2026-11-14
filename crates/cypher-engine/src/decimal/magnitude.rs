//! Private unsigned intermediates, not a wider public numeric type.

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct Magnitude {
    high: u128,
    low: u128,
}

impl From<u128> for Magnitude {
    fn from(low: u128) -> Self {
        Self { high: 0, low }
    }
}

impl Magnitude {
    pub(super) const ZERO: Self = Self { high: 0, low: 0 };
    pub(super) const ONE: Self = Self { high: 0, low: 1 };

    pub(super) fn to_u128(self) -> Option<u128> {
        (self.high == 0).then_some(self.low)
    }

    pub(super) fn to_f64(self) -> f64 {
        (self.high as f64).mul_add(2f64.powi(128), self.low as f64)
    }

    pub(super) fn is_odd(self) -> bool {
        self.low & 1 == 1
    }

    /// A 128-bit product fits exactly in 256 bits.
    pub(super) fn product(a: u128, b: u128) -> Self {
        let (a_hi, a_lo) = (a >> 64, a & u128::from(u64::MAX));
        let (b_hi, b_lo) = (b >> 64, b & u128::from(u64::MAX));
        let low = a_lo * b_lo;
        let middle = a_lo * b_hi;
        let (middle, carry) = middle.overflowing_add(a_hi * b_lo);
        let (low, low_carry) = low.overflowing_add(middle << 64);
        let high = a_hi * b_hi + (middle >> 64) + (u128::from(carry) << 64) + u128::from(low_carry);
        Self { high, low }
    }

    pub(super) fn checked_add(self, other: Self) -> Option<Self> {
        let (low, carry) = self.low.overflowing_add(other.low);
        Some(Self {
            high: self.high.checked_add(other.high)?.checked_add(u128::from(carry))?,
            low,
        })
    }

    pub(super) fn checked_sub(self, other: Self) -> Option<Self> {
        let (low, borrow) = self.low.overflowing_sub(other.low);
        Some(Self {
            high: self.high.checked_sub(other.high)?.checked_sub(u128::from(borrow))?,
            low,
        })
    }

    pub(super) fn checked_scale(self, shift: u32) -> Option<Self> {
        if shift == 0 {
            return Some(self);
        }
        let factor = 10_u128.checked_pow(shift)?;
        let carry = Self::product(self.low, factor);
        Some(Self {
            high: self.high.checked_mul(factor)?.checked_add(carry.high)?,
            low: carry.low,
        })
    }

    /// Exact quotient and remainder; zero divisors have no quotient.
    pub(super) fn divide(self, divisor: u128) -> Option<(Self, u128)> {
        if divisor == 0 {
            return None;
        }
        let (high, mut remainder) = (self.high / divisor, self.high % divisor);
        let mut low = 0_u128;
        for bit in (0..128).rev() {
            // remainder < divisor, so doubling it overflows at most one bit.
            let carry = remainder >> 127;
            remainder = (remainder << 1) | ((self.low >> bit) & 1);
            if carry == 1 || remainder >= divisor {
                remainder = remainder.wrapping_sub(divisor);
                low |= 1 << bit;
            }
        }
        Some((Self { high, low }, remainder))
    }

    /// The divisor is nonzero and below 2^255; decimal scale alignment keeps
    /// it within that bound, so the running remainder can double exactly.
    pub(super) fn remainder(self, divisor: Self) -> Self {
        debug_assert!(divisor != Self::ZERO && divisor.high >> 127 == 0);
        let mut remainder = Self::ZERO;
        for bit in (0..256).rev() {
            let next = if bit >= 128 {
                (self.high >> (bit - 128)) & 1
            } else {
                (self.low >> bit) & 1
            };
            remainder = Self {
                high: (remainder.high << 1) | (remainder.low >> 127),
                low: (remainder.low << 1) | next,
            };
            if remainder >= divisor {
                remainder = remainder.checked_sub(divisor).expect("the remainder is at least the divisor");
            }
        }
        remainder
    }

    pub(super) fn checked_shl(self, bits: u32) -> Option<Self> {
        let Self { high, low } = self;
        if bits == 0 {
            return Some(self);
        }
        if bits >= 256 {
            return (self == Self::ZERO).then_some(Self::ZERO);
        }
        // Exactly 128 moves the low half up whole; a u128 cannot shift by 128.
        if bits == 128 {
            return (high == 0).then_some(Self { high: low, low: 0 });
        }
        if bits > 128 {
            return (high == 0 && low >> (256 - bits) == 0).then(|| Self {
                high: low << (bits - 128),
                low: 0,
            });
        }
        (high >> (128 - bits) == 0).then(|| Self {
            high: (high << bits) | (low >> (128 - bits)),
            low: low << bits,
        })
    }

    /// The floor after a right shift, and whether any nonzero bit fell off.
    pub(super) fn shr(self, bits: u32) -> (Self, bool) {
        let Self { high, low } = self;
        if bits == 0 {
            return (self, false);
        }
        if bits >= 256 {
            return (Self::ZERO, self != Self::ZERO);
        }
        if bits >= 128 {
            let dropped = low != 0 || high & ((1_u128 << (bits - 128)) - 1) != 0;
            return (Self::from(if bits == 128 { high } else { high >> (bits - 128) }), dropped);
        }
        let dropped = low & ((1_u128 << bits) - 1) != 0;
        (
            Self {
                high: high >> bits,
                low: (low >> bits) | (high << (128 - bits)),
            },
            dropped,
        )
    }
}
