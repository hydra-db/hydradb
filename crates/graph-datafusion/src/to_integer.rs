use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::{Array, BooleanArray, Decimal128Array, Decimal128Builder, Float64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use hydradb_graph_types::{
    canonical_path_field, canonical_relationship_binding_schema, canonical_value_data_type, value_from_array, CypherValue,
};

const PRECISION: u8 = 38;
const SCALE: i8 = 0;

pub fn to_integer(expression: Expr) -> Expr {
    ScalarUDF::from(ToInteger).call(vec![expression])
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct ToInteger;

impl ScalarUDFImpl for ToInteger {
    fn name(&self) -> &str {
        "cypher_to_integer"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(|| Signature::user_defined(Volatility::Immutable))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Decimal128(PRECISION, SCALE))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [argument] = arg_types else {
            return Err(DataFusionError::Plan("toInteger expects one argument".into()));
        };
        if argument == &canonical_value_data_type() || is_entity_type(argument) {
            return Ok(vec![argument.clone()]);
        }
        let coerced = match argument {
            DataType::Null => DataType::Utf8,
            DataType::Boolean | DataType::Decimal128(_, _) | DataType::Float64 | DataType::Utf8 => argument.clone(),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32 => DataType::Decimal128(PRECISION, SCALE),
            DataType::Float32 => DataType::Float64,
            DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
            _ => return Err(DataFusionError::Plan("toInteger conversion failed".into())),
        };
        Ok(vec![coerced])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [argument] = args.args.as_slice() else {
            return Err(DataFusionError::Internal("toInteger expects one argument".into()));
        };
        match argument {
            ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                scalar_to_integer(value)?,
                PRECISION,
                SCALE,
            ))),
            ColumnarValue::Array(array) => {
                let mut output = Decimal128Builder::with_capacity(array.len());
                for row in 0..array.len() {
                    output.append_option(array_to_integer(array, row)?);
                }
                let output = output
                    .finish()
                    .with_precision_and_scale(PRECISION, SCALE)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                Ok(ColumnarValue::Array(Arc::new(output)))
            }
        }
    }
}

fn scalar_to_integer(value: &ScalarValue) -> Result<Option<i128>> {
    if value.is_null() {
        return Ok(None);
    }
    let data_type = value.data_type();
    if data_type == canonical_value_data_type() {
        return array_to_integer(&value.to_array_of_size(1)?, 0);
    }
    match value {
        ScalarValue::Null => Ok(None),
        ScalarValue::Boolean(value) => Ok(value.map(i128::from)),
        ScalarValue::Decimal128(value, _, _) => Ok(*value),
        ScalarValue::Float64(value) => value.map(truncate_float).transpose(),
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) => match value.as_deref() {
            Some(value) => parse_string(value),
            None => Ok(None),
        },
        _ => Err(DataFusionError::Execution("toInteger conversion failed".into())),
    }
}

fn array_to_integer(array: &Arc<dyn Array>, row: usize) -> Result<Option<i128>> {
    if array.data_type() == &canonical_value_data_type() {
        let value = value_from_array(array.as_ref(), row)
            .map_err(|error| DataFusionError::Execution(format!("cannot decode canonical graph value: {error}")))?;
        return graph_value_to_integer(Some(&value));
    }
    if array.is_null(row) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Boolean => Ok(Some(i128::from(
            array.as_any().downcast_ref::<BooleanArray>().expect("boolean type").value(row),
        ))),
        DataType::Decimal128(_, _) => Ok(Some(
            array.as_any().downcast_ref::<Decimal128Array>().expect("decimal type").value(row),
        )),
        DataType::Float64 => truncate_float(array.as_any().downcast_ref::<Float64Array>().expect("float type").value(row)).map(Some),
        DataType::Utf8 => parse_string(array.as_any().downcast_ref::<StringArray>().expect("string type").value(row)),
        _ => Err(DataFusionError::Execution("toInteger conversion failed".into())),
    }
}

fn graph_value_to_integer(value: Option<&CypherValue>) -> Result<Option<i128>> {
    match to_integer_graph_value(value)? {
        CypherValue::Integer(value) => Ok(Some(value)),
        CypherValue::Null => Ok(None),
        _ => Err(DataFusionError::Internal("toInteger returned a non-integer graph value".into())),
    }
}

fn is_entity_type(data_type: &DataType) -> bool {
    data_type == &DataType::UInt64
        || data_type == canonical_relationship_binding_schema().field(0).data_type()
        || data_type == canonical_path_field("path", true).data_type()
}

fn parse_string(value: &str) -> Result<Option<i128>> {
    let value = value.trim();
    if let Ok(integer) = value.parse::<i64>() {
        return Ok(Some(i128::from(integer)));
    }
    match value.parse::<f64>() {
        Ok(float) if float.is_finite() => truncate_float(float).map(Some),
        _ => Ok(None),
    }
}

fn truncate_decimal_text(value: &str) -> Result<i128> {
    let value = value.trim();
    let (negative, unsigned) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    let mut exponent_parts = unsigned.split(['e', 'E']);
    let significand = exponent_parts.next().unwrap_or_default();
    let exponent = exponent_parts
        .next()
        .map(|value| value.parse::<i32>())
        .transpose()
        .map_err(|_| DataFusionError::Execution("toInteger conversion failed".into()))?
        .unwrap_or(0);
    if exponent_parts.next().is_some() {
        return Err(DataFusionError::Execution("toInteger conversion failed".into()));
    }
    let mut decimal_parts = significand.split('.');
    let whole = decimal_parts.next().unwrap_or_default();
    let fraction = decimal_parts.next().unwrap_or_default();
    if decimal_parts.next().is_some()
        || (whole.is_empty() && fraction.is_empty())
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(DataFusionError::Execution("toInteger conversion failed".into()));
    }

    let digits = format!("{whole}{fraction}");
    if digits.bytes().all(|byte| byte == b'0') {
        return Ok(0);
    }
    let integer_digits = i64::try_from(whole.len()).unwrap_or(i64::MAX).saturating_add(i64::from(exponent));
    if integer_digits <= 0 {
        return Ok(0);
    }
    let integer_digits =
        usize::try_from(integer_digits).map_err(|_| DataFusionError::Execution("toInteger conversion is out of range".into()))?;
    let leading_zeroes = digits.bytes().take_while(|byte| *byte == b'0').count().min(integer_digits);
    let significant_digits = integer_digits.saturating_sub(leading_zeroes);
    if significant_digits == 0 {
        return Ok(0);
    }
    if significant_digits > 19 {
        return Err(DataFusionError::Execution("toInteger conversion is out of range".into()));
    }
    let mut integer = String::with_capacity(significant_digits + usize::from(negative));
    if negative {
        integer.push('-');
    }
    if integer_digits <= digits.len() {
        integer.push_str(&digits[leading_zeroes..integer_digits]);
    } else {
        integer.push_str(&digits[leading_zeroes..]);
        integer.extend(std::iter::repeat_n('0', integer_digits - digits.len()));
    }
    integer
        .parse::<i64>()
        .map(i128::from)
        .map_err(|_| DataFusionError::Execution("toInteger conversion is out of range".into()))
}

fn truncate_float(value: f64) -> Result<i128> {
    const I64_EXCLUSIVE_UPPER: f64 = 9_223_372_036_854_775_808.0;
    let truncated = value.trunc();
    if !truncated.is_finite() || truncated < -I64_EXCLUSIVE_UPPER || truncated >= I64_EXCLUSIVE_UPPER {
        return Err(DataFusionError::Execution("toInteger conversion is out of range".into()));
    }
    Ok(truncated as i128)
}

pub fn to_integer_graph_value(value: Option<&CypherValue>) -> Result<CypherValue> {
    match value {
        Some(CypherValue::Integer(value)) => Ok(CypherValue::Integer(*value)),
        Some(CypherValue::Boolean(value)) => Ok(CypherValue::Integer(i128::from(*value))),
        Some(CypherValue::Float(value)) => truncate_float(*value).map(CypherValue::Integer),
        Some(CypherValue::String(value)) => Ok(parse_string(value)?.map_or(CypherValue::Null, CypherValue::Integer)),
        Some(CypherValue::Decimal(value)) => truncate_decimal_text(value).map(CypherValue::Integer),
        Some(CypherValue::Null | CypherValue::Missing) | None => Ok(CypherValue::Null),
        Some(
            CypherValue::Node(_)
            | CypherValue::Relationship(_)
            | CypherValue::NodeEntity(_)
            | CypherValue::RelationshipEntity(_)
            | CypherValue::Path(_),
        ) => Err(DataFusionError::Execution("toInteger conversion failed".into())),
        _ => Err(DataFusionError::Execution("toInteger conversion failed".into())),
    }
}
