//! DataFusion expressions for comparisons in the canonical graph value domain.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::{Array, BooleanBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use hydradb_graph_plan::GraphBinaryOperator;
use hydradb_graph_types::{
    canonical_value_data_type, semantic_cmp, semantic_eq_three_valued, semantic_list_membership, value_from_array, CypherValue, NodeId,
};

/// Build a comparison expression that uses canonical graph semantics.
///
/// Returns `None` for operators outside equality and ordering comparisons.
pub fn semantic_comparison(operator: GraphBinaryOperator, left: Expr, right: Expr) -> Option<Expr> {
    is_semantic_comparison_operator(operator).then(|| ScalarUDF::from(SemanticComparison { operator }).call(vec![left, right]))
}

pub fn is_semantic_comparison_operator(operator: GraphBinaryOperator) -> bool {
    matches!(
        operator,
        GraphBinaryOperator::Equal
            | GraphBinaryOperator::NotEqual
            | GraphBinaryOperator::LessThan
            | GraphBinaryOperator::LessThanOrEqual
            | GraphBinaryOperator::GreaterThan
            | GraphBinaryOperator::GreaterThanOrEqual
    )
}

pub fn semantic_membership(left: Expr, right: Expr) -> Expr {
    ScalarUDF::from(SemanticMembership).call(vec![left, right])
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct SemanticMembership;

impl ScalarUDFImpl for SemanticMembership {
    fn name(&self) -> &str {
        "cypher_semantic_membership"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(|| Signature::any(2, Volatility::Immutable))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left, right] = args.args.as_slice() else {
            return Err(DataFusionError::Internal("semantic membership expects two arguments".to_owned()));
        };
        let left = ComparisonOperand::try_new(left.clone())?;
        let right = ComparisonOperand::try_new(right.clone())?;

        if left.is_scalar() && right.is_scalar() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(membership_optional_values(
                left.value_at(0)?,
                right.value_at(0)?,
            )?)));
        }

        let mut output = BooleanBuilder::with_capacity(args.number_rows);
        for row in 0..args.number_rows {
            match membership_optional_values(left.value_at(row)?, right.value_at(row)?)? {
                Some(value) => output.append_value(value),
                None => output.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(output.finish())))
    }
}

fn membership_optional_values(left: Option<Cow<'_, CypherValue>>, right: Option<Cow<'_, CypherValue>>) -> Result<Option<bool>> {
    let Some(right) = right else {
        return Ok(None);
    };
    let CypherValue::List(values) = right.as_ref() else {
        return Err(DataFusionError::Execution("IN requires a list on the right-hand side".into()));
    };
    if values.is_empty() {
        return Ok(Some(false));
    }
    let Some(left) = left else {
        return Ok(None);
    };
    Ok(semantic_list_membership(left.as_ref(), values))
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct SemanticComparison {
    operator: GraphBinaryOperator,
}

impl ScalarUDFImpl for SemanticComparison {
    fn name(&self) -> &str {
        match self.operator {
            GraphBinaryOperator::Equal => "cypher_semantic_equal",
            GraphBinaryOperator::NotEqual => "cypher_semantic_not_equal",
            GraphBinaryOperator::LessThan => "cypher_semantic_less_than",
            GraphBinaryOperator::LessThanOrEqual => "cypher_semantic_less_than_or_equal",
            GraphBinaryOperator::GreaterThan => "cypher_semantic_greater_than",
            GraphBinaryOperator::GreaterThanOrEqual => "cypher_semantic_greater_than_or_equal",
            _ => unreachable!("only comparison operators construct this UDF"),
        }
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(|| Signature::any(2, Volatility::Immutable))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left, right] = args.args.as_slice() else {
            return Err(DataFusionError::Internal("semantic comparison expects two arguments".to_owned()));
        };
        let left = ComparisonOperand::try_new(left.clone())?;
        let right = ComparisonOperand::try_new(right.clone())?;

        if left.is_scalar() && right.is_scalar() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(compare_optional_values(
                self.operator,
                left.value_at(0)?,
                right.value_at(0)?,
            ))));
        }

        let mut output = BooleanBuilder::with_capacity(args.number_rows);
        for row in 0..args.number_rows {
            match (left.value_at(row)?, right.value_at(row)?) {
                (Some(left), Some(right)) => match compare_values(self.operator, left.as_ref(), right.as_ref()) {
                    Some(value) => output.append_value(value),
                    None => output.append_null(),
                },
                _ => output.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(output.finish())))
    }
}

pub(crate) enum ComparisonOperand {
    Scalar(Option<CypherValue>),
    Array(Arc<dyn Array>),
}

impl ComparisonOperand {
    pub(crate) fn try_new(value: ColumnarValue) -> Result<Self> {
        match value {
            ColumnarValue::Scalar(value) if value.data_type() == canonical_value_data_type() => {
                let array = value.to_array_of_size(1)?;
                Ok(Self::Scalar(array_value_to_graph(array.as_ref(), 0)?))
            }
            ColumnarValue::Scalar(value) => Ok(Self::Scalar(value_to_graph(value)?)),
            ColumnarValue::Array(array) => Ok(Self::Array(array)),
        }
    }

    pub(crate) fn is_scalar(&self) -> bool {
        matches!(self, Self::Scalar(_))
    }

    pub(crate) fn value_at(&self, row: usize) -> Result<Option<Cow<'_, CypherValue>>> {
        match self {
            Self::Scalar(Some(value)) => Ok(Some(Cow::Borrowed(value))),
            Self::Scalar(None) => Ok(None),
            Self::Array(array) if array.is_null(row) => Ok(None),
            Self::Array(array) if array.data_type() == &canonical_value_data_type() => {
                Ok(array_value_to_graph(array.as_ref(), row)?.map(Cow::Owned))
            }
            Self::Array(array) => Ok(value_to_graph(ScalarValue::try_from_array(array.as_ref(), row)?)?.map(Cow::Owned)),
        }
    }
}

fn array_value_to_graph(array: &dyn Array, row: usize) -> Result<Option<CypherValue>> {
    let value = value_from_array(array, row)
        .map_err(|error| DataFusionError::Execution(format!("cannot decode canonical graph value: {error}")))?;
    Ok(match value {
        CypherValue::Null | CypherValue::Missing => None,
        value => Some(value),
    })
}

fn value_to_graph(value: ScalarValue) -> Result<Option<CypherValue>> {
    let value = match value {
        ScalarValue::Null
        | ScalarValue::Boolean(None)
        | ScalarValue::Float16(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Decimal32(None, _, _)
        | ScalarValue::Decimal64(None, _, _)
        | ScalarValue::Decimal128(None, _, _)
        | ScalarValue::Decimal256(None, _, _)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None)
        | ScalarValue::Utf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Binary(None)
        | ScalarValue::BinaryView(None)
        | ScalarValue::LargeBinary(None) => return Ok(None),
        ScalarValue::Boolean(Some(value)) => CypherValue::Boolean(value),
        ScalarValue::Float16(Some(value)) => CypherValue::Float(f64::from(f32::from(value))),
        ScalarValue::Float32(Some(value)) => CypherValue::Float(f64::from(value)),
        ScalarValue::Float64(Some(value)) => CypherValue::Float(value),
        ScalarValue::Decimal32(Some(value), _, scale) => CypherValue::Decimal(scaled_decimal(value, scale)),
        ScalarValue::Decimal64(Some(value), _, scale) => CypherValue::Decimal(scaled_decimal(value, scale)),
        ScalarValue::Decimal128(Some(value), _, scale) => CypherValue::Decimal(scaled_decimal(value, scale)),
        ScalarValue::Decimal256(Some(value), _, scale) => CypherValue::Decimal(scaled_decimal(value, scale)),
        ScalarValue::Int8(Some(value)) => CypherValue::Integer(value.into()),
        ScalarValue::Int16(Some(value)) => CypherValue::Integer(value.into()),
        ScalarValue::Int32(Some(value)) => CypherValue::Integer(value.into()),
        ScalarValue::Int64(Some(value)) => CypherValue::Integer(value.into()),
        ScalarValue::UInt8(Some(value)) => CypherValue::Integer(value.into()),
        ScalarValue::UInt16(Some(value)) => CypherValue::Integer(value.into()),
        ScalarValue::UInt32(Some(value)) => CypherValue::Integer(value.into()),
        ScalarValue::UInt64(Some(value)) => CypherValue::Node(NodeId(value)),
        ScalarValue::Utf8(Some(value)) | ScalarValue::Utf8View(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            CypherValue::String(value)
        }
        ScalarValue::Binary(Some(value)) | ScalarValue::BinaryView(Some(value)) | ScalarValue::LargeBinary(Some(value)) => {
            CypherValue::Bytes(value)
        }
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "semantic comparison does not support native scalar type {}",
                other.data_type()
            )));
        }
    };
    Ok(Some(value))
}

fn scaled_decimal(value: impl ToString, scale: i8) -> String {
    let value = value.to_string();
    let (sign, digits) = value.strip_prefix('-').map_or(("", value.as_str()), |digits| ("-", digits));
    if scale <= 0 {
        return format!("{sign}{digits}{}", "0".repeat(scale.unsigned_abs().into()));
    }

    let scale = usize::from(scale.unsigned_abs());
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let decimal_point = padded.len() - scale;
    format!("{sign}{}.{}", &padded[..decimal_point], &padded[decimal_point..])
}

fn compare_optional_values(
    operator: GraphBinaryOperator,
    left: Option<Cow<'_, CypherValue>>,
    right: Option<Cow<'_, CypherValue>>,
) -> Option<bool> {
    compare_values(operator, left.as_deref()?, right.as_deref()?)
}

fn compare_values(operator: GraphBinaryOperator, left: &CypherValue, right: &CypherValue) -> Option<bool> {
    Some(match operator {
        GraphBinaryOperator::Equal => return semantic_eq_three_valued(left, right),
        GraphBinaryOperator::NotEqual => return semantic_eq_three_valued(left, right).map(|equal| !equal),
        GraphBinaryOperator::LessThan => semantic_cmp(left, right) == Ordering::Less,
        GraphBinaryOperator::LessThanOrEqual => semantic_cmp(left, right) != Ordering::Greater,
        GraphBinaryOperator::GreaterThan => semantic_cmp(left, right) == Ordering::Greater,
        GraphBinaryOperator::GreaterThanOrEqual => semantic_cmp(left, right) != Ordering::Less,
        _ => unreachable!("only comparison operators construct this UDF"),
    })
}
