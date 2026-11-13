use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::{Array, Decimal128Array, Decimal128Builder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};

const PRECISION: u8 = 38;
const SCALE: i8 = 0;

pub fn integer_division(left: Expr, right: Expr) -> Expr {
    ScalarUDF::from(IntegerDivision).call(vec![left, right])
}

#[derive(Debug, Eq, Hash, PartialEq)]
struct IntegerDivision;

impl ScalarUDFImpl for IntegerDivision {
    fn name(&self) -> &str {
        "cypher_integer_divide"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: OnceLock<Signature> = OnceLock::new();
        SIGNATURE.get_or_init(|| {
            Signature::exact(
                vec![DataType::Decimal128(PRECISION, SCALE), DataType::Decimal128(PRECISION, SCALE)],
                Volatility::Immutable,
            )
        })
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Decimal128(PRECISION, SCALE))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left, right] = args.args.as_slice() else {
            return Err(DataFusionError::Internal("integer division expects two arguments".to_owned()));
        };
        if let (ColumnarValue::Scalar(ScalarValue::Decimal128(left, _, _)), ColumnarValue::Scalar(ScalarValue::Decimal128(right, _, _))) =
            (left, right)
        {
            return Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                divide_options(*left, *right)?,
                PRECISION,
                SCALE,
            )));
        }

        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let left = decimal_array(&arrays[0])?;
        let right = decimal_array(&arrays[1])?;
        let mut output = Decimal128Builder::with_capacity(left.len());
        for row in 0..left.len() {
            let left = (!left.is_null(row)).then(|| left.value(row));
            let right = (!right.is_null(row)).then(|| right.value(row));
            output.append_option(divide_options(left, right)?);
        }
        let output = output
            .finish()
            .with_precision_and_scale(PRECISION, SCALE)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        Ok(ColumnarValue::Array(Arc::new(output)))
    }
}

fn decimal_array(array: &Arc<dyn Array>) -> Result<&Decimal128Array> {
    array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| DataFusionError::Internal("integer division received a non-decimal argument".to_owned()))
}

fn divide_options(left: Option<i128>, right: Option<i128>) -> Result<Option<i128>> {
    match (left, right) {
        (Some(_), Some(0)) => Err(DataFusionError::Execution("division by zero in Cypher expression".to_owned())),
        (Some(left), Some(right)) => left
            .checked_div(right)
            .map(Some)
            .ok_or_else(|| DataFusionError::Execution("integer division overflow in Cypher expression".to_owned())),
        _ => Ok(None),
    }
}
