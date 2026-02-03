//! Color mapping utilities for SVG visualization.

use std::fmt;

/// Simple RGB color.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Rgb {
    pub(crate) r: u8,
    pub(crate) g: u8,
    pub(crate) b: u8,
}

impl fmt::Display for Rgb {
    /// Format as CSS: rgb(r,g,b)
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rgb({},{},{})", self.r, self.g, self.b)
    }
}

/// HSL color: h in degrees, s and l in [0.0, 1.0].
#[derive(Clone, Copy, Debug)]
pub(crate) struct Hsl {
    pub(crate) h: f64,
    pub(crate) s: f64,
    pub(crate) l: f64,
}

impl fmt::Display for Hsl {
    /// Format as CSS HSL:
    ///   hsl({h:.1},{s:.0}%,{l:.0}%)
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // normalize hue into [0,360)
        let h = (self.h % 360.0 + 360.0) % 360.0;
        let s = (self.s * 100.0).clamp(0.0, 100.0);
        let l = (self.l * 100.0).clamp(0.0, 100.0);
        write!(f, "hsl({:.1},{:.0}%,{:.0}%)", h, s, l)
    }
}

/// Partisan color ramp for lean in [-1.0, 1.0].
pub(crate) fn partisan_color(lean: f64) -> Rgb {
    // Handle NaN / infinities: fall back to neutral gray.
    if !lean.is_finite() { return Rgb { r: 150, g: 150, b: 150 } }

    // Clamp lean to the nominal domain.
    let x = lean.clamp(-1.0, 1.0);

    // (lower, upper, color) taken from <ranges> + <symbols> in the QML.
    // Ranges are [lower, upper), except the last which includes upper=100.
    const BREAKS: &[(f64, f64, Rgb)] = &[
        (-1.0,  -0.5,  Rgb { r: 202, g:   0, b:  32 }),
        (-0.5,  -0.3,  Rgb { r: 211, g:  33, b:  58 }),
        (-0.3,  -0.2,  Rgb { r: 220, g:  66, b:  83 }),
        (-0.2,  -0.15, Rgb { r: 229, g:  99, b: 109 }),
        (-0.15, -0.12, Rgb { r: 238, g: 132, b: 135 }),
        (-0.12, -0.09, Rgb { r: 243, g: 160, b: 157 }),
        (-0.09, -0.06, Rgb { r: 241, g: 176, b: 174 }),
        (-0.06, -0.04, Rgb { r: 238, g: 192, b: 190 }),
        (-0.04, -0.02, Rgb { r: 236, g: 208, b: 207 }),
        (-0.02,  0.00, Rgb { r: 233, g: 224, b: 224 }),
        ( 0.00,  0.02, Rgb { r: 223, g: 228, b: 231 }),
        ( 0.02,  0.04, Rgb { r: 205, g: 221, b: 229 }),
        ( 0.04,  0.06, Rgb { r: 187, g: 213, b: 227 }),
        ( 0.06,  0.09, Rgb { r: 168, g: 206, b: 225 }),
        ( 0.09,  0.12, Rgb { r: 150, g: 199, b: 222 }),
        ( 0.12,  0.15, Rgb { r: 123, g: 183, b: 215 }),
        ( 0.15,  0.2,  Rgb { r:  94, g: 166, b: 205 }),
        ( 0.2,   0.3,  Rgb { r:  64, g: 148, b: 195 }),
        ( 0.3,   0.5,  Rgb { r:  35, g: 131, b: 186 }),
        ( 0.5,   1.0,  Rgb { r:   5, g: 113, b: 176 }),
    ];

    // Find the matching bucket. Use [lower, upper), except include the last upper.
    for &(lo, hi, color) in BREAKS {
        if x >= lo && (x < hi || (hi == 100.0 && x <= hi)) { return color }
    }

    // Fallback (should be unreachable if breaks cover [-100,100]).
    Rgb { r: 150, g: 150, b: 150 }
}

const GOLDEN_ANGLE: f64 = 137.50776405;

pub(crate) fn golden_angle_color(index: usize) -> Hsl {
    Hsl { h: ((index as f64) * GOLDEN_ANGLE) % 360.0, s: 0.70, l: 0.55 }
}
