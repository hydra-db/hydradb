use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use hydradb_graph_types::{canonical_value_data_type, value_array, CypherValue, DateValue};

use crate::semantic_comparison::ComparisonOperand;

pub fn date_value(argument: Expr) -> Expr {
    ScalarUDF::from(DateValueConstructor).call(vec![argument])
}

pub fn list_value(arguments: Vec<Expr>) -> Expr {
    ScalarUDF::from(ListValueConstructor).call(arguments)
}

/// The internal function name `list[index]` binds to. It is not a name a
/// Cypher function call can spell, so it never shadows a user function.
pub const LIST_ELEMENT_FUNCTION: &str = "[]";

/// `list[index]`, Cypher's list element access, as a DataFusion expression.
pub fn list_element_value(list: Expr, index: Expr) -> Expr {
    ScalarUDF::from(ListElementAccess).call(vec![list, index])
}

/// `list[index]` on one pair of values, shared by queries and mutations so
/// both follow the same rules, which match the experimental engine:
///
/// - a null list or a null index is null;
/// - the index counts from 0, and a negative index counts back from the end;
/// - an index past either end is null, not an error;
/// - anything but a list on the left, or anything but an integer as the
///   index, is an error.
pub fn list_element(list: &CypherValue, index: &CypherValue) -> Result<CypherValue> {
    if matches!(list, CypherValue::Null | CypherValue::Missing) || matches!(index, CypherValue::Null | CypherValue::Missing) {
        return Ok(CypherValue::Null);
    }
    let CypherValue::List(values) = list else {
        return Err(DataFusionError::Execution("list element access requires a list".into()));
    };
    let CypherValue::Integer(index) = index else {
        return Err(DataFusionError::Execution("a list index must be an integer".into()));
    };
    let length = values.len() as i128;
    let position = if *index < 0 { length + index } else { *index };
    if !(0..length).contains(&position) {
        return Ok(CypherValue::Null);
    }
    Ok(values[position as usize].clone())
}

/// DataFusion carries Cypher integer literals as `Decimal128(38, 0)`, which
/// reach a UDF as an integral `CypherValue::Decimal`. The Cypher binder
/// rejects decimal literals, so an integral decimal here is an integer.
fn integral_literal(value: CypherValue) -> CypherValue {
    match &value {
        CypherValue::Decimal(digits) if !digits.contains('.') => digits.parse().map(CypherValue::Integer).unwrap_or(value),
        _ => value,
    }
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct ListElementAccess;

impl ScalarUDFImpl for ListElementAccess {
    fn name(&self) -> &str {
        "cypher_list_element"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(|| Signature::any(2, Volatility::Immutable))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(canonical_value_data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [list, index] = args.args.as_slice() else {
            return Err(DataFusionError::Internal("list element access expects a list and an index".into()));
        };
        let list = ComparisonOperand::try_new(list.clone())?;
        let index = ComparisonOperand::try_new(index.clone())?;
        let scalar = list.is_scalar() && index.is_scalar();
        let rows = if scalar { 1 } else { args.number_rows };
        let mut values = Vec::with_capacity(rows);
        for row in 0..rows {
            // Borrow the list: a parameter list is shared by every row, and
            // only the selected element needs copying.
            let list = list.value_at(row)?;
            let index = index.value_at(row)?.map_or(CypherValue::Null, |value| value.into_owned());
            values.push(list_element(
                list.as_deref().unwrap_or(&CypherValue::Null),
                &integral_literal(index),
            )?);
        }
        canonical_output(values, scalar)
    }
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct DateValueConstructor;

impl ScalarUDFImpl for DateValueConstructor {
    fn name(&self) -> &str {
        "cypher_date_value"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(|| Signature::any(1, Volatility::Immutable))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(canonical_value_data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [argument] = args.args.as_slice() else {
            return Err(DataFusionError::Internal("date constructor expects one argument".into()));
        };
        let argument = ComparisonOperand::try_new(argument.clone())?;
        let scalar = argument.is_scalar();
        let rows = if scalar { 1 } else { args.number_rows };
        let mut values = Vec::with_capacity(rows);
        for row in 0..rows {
            let value = match argument.value_at(row)? {
                None => CypherValue::Null,
                Some(value) => match value.as_ref() {
                    CypherValue::String(value) => DateValue::parse_iso(value).map(CypherValue::Date).ok_or_else(|| {
                        DataFusionError::Execution(format!("invalid date `{value}`; expected an ISO 8601 date such as YYYY-MM-DD"))
                    })?,
                    CypherValue::Date(value) => CypherValue::Date(*value),
                    _ => return Err(DataFusionError::Execution("date requires one string or date argument".into())),
                },
            };
            values.push(value);
        }
        canonical_output(values, scalar)
    }
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct ListValueConstructor;

impl ScalarUDFImpl for ListValueConstructor {
    fn name(&self) -> &str {
        "cypher_list_value"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(|| Signature::variadic_any(Volatility::Immutable))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(canonical_value_data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arguments = args.args.into_iter().map(ListOperand::try_new).collect::<Result<Vec<_>>>()?;
        let scalar = arguments.iter().all(ListOperand::is_scalar);
        let rows = if scalar { 1 } else { args.number_rows };
        let mut values = Vec::with_capacity(rows);
        for row in 0..rows {
            let items = arguments
                .iter()
                .map(|argument| argument.value_at(row))
                .collect::<Result<Vec<_>>>()?;
            values.push(CypherValue::List(items));
        }
        canonical_output(values, scalar)
    }
}

struct ListOperand {
    value: ComparisonOperand,
    integer_decimal: bool,
}

impl ListOperand {
    fn try_new(value: ColumnarValue) -> Result<Self> {
        let integer_decimal = value.data_type() == DataType::Decimal128(38, 0);
        Ok(Self {
            value: ComparisonOperand::try_new(value)?,
            integer_decimal,
        })
    }

    fn is_scalar(&self) -> bool {
        self.value.is_scalar()
    }

    fn value_at(&self, row: usize) -> Result<CypherValue> {
        let value = self.value.value_at(row)?.map_or(CypherValue::Null, |value| value.into_owned());
        if !self.integer_decimal {
            return Ok(value);
        }
        let CypherValue::Decimal(value) = value else {
            return Ok(value);
        };
        value
            .parse::<i128>()
            .map(CypherValue::Integer)
            .map_err(|error| DataFusionError::Internal(format!("cannot decode integer list value `{value}`: {error}")))
    }
}

fn canonical_output(values: Vec<CypherValue>, scalar: bool) -> Result<ColumnarValue> {
    let array = Arc::new(value_array(&values).map_err(|error| DataFusionError::Execution(error.to_string()))?) as ArrayRef;
    if scalar {
        ScalarValue::try_from_array(array.as_ref(), 0).map(ColumnarValue::Scalar)
    } else {
        Ok(ColumnarValue::Array(array))
    }
}
