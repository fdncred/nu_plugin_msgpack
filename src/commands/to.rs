use crate::MsgPackPlugin;
use nu_plugin::{EngineInterface, EvaluatedCall, SimplePluginCommand};
use nu_protocol::{Category, LabeledError, Signature, Span, SyntaxShape, Type, Value};

pub struct ToMsgpack;

impl SimplePluginCommand for ToMsgpack {
    type Plugin = MsgPackPlugin;

    fn name(&self) -> &str {
        "to msgpack"
    }

    fn usage(&self) -> &str {
        "Converts data to msgpack."
    }
    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .named(
                "compression",
                SyntaxShape::Int,
                "Brotli Encoder Mode (0 - 11)",
                Some('c'),
            )
            .category(Category::Formats)
            .input_output_type(Type::Any, Type::Table(vec![]))
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["msgpack", "plugins"]
    }

    fn run(
        &self,
        _plugin: &MsgPackPlugin,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        input: &Value,
    ) -> Result<Value, LabeledError> {
        let compression_param: Option<Value> = call.get_flag("compression")?;
        let compression = match compression_param {
            Some(Value::Int { val, .. }) => val as i32,
            _ => 5, // 5 seemed to be a nice level of compression for the time
        };
        let msgpack_value = nu_to_rmpv(input.clone(), compression)?;
        let mut encoded = vec![];
        rmpv::encode::write_value(&mut encoded, &msgpack_value)
            .expect("encoding to vec can't fail, right?");
        Ok(Value::binary(encoded, Span::unknown()))
    }
}

/// Convert [nu_protocol::Value] to a [rmpv::Value].
pub fn nu_to_rmpv(value: Value, compression: i32) -> Result<rmpv::Value, LabeledError> {
    let span = value.span();
    Ok(match value {
        Value::Bool { val, .. } => val.into(),
        Value::Int { val, .. } => val.into(),
        Value::Float { val, .. } => val.into(),
        Value::String { val, .. } => {
            let mut compressed = Vec::<u8>::new();
            brotli::BrotliCompress(
                &mut val.as_bytes(),
                &mut compressed,
                &brotli::enc::BrotliEncoderParams {
                    quality: compression,
                    ..Default::default()
                },
            )
            .map_err(|err| {
                LabeledError::new(format!("Error {err}"))
                    .with_label("Error compressing string with Brotli", span)
            })?;
            let bin = Value::binary(compressed, span);
            nu_to_rmpv(bin, compression)?
        }
        Value::Binary { val, .. } => val.into(),
        Value::Nothing { .. } => rmpv::Value::Nil,
        Value::List { vals, .. } => {
            let vals: Result<_, _> = vals
                .into_iter()
                .map(|r| nu_to_rmpv(r, compression))
                .collect();
            rmpv::Value::Array(vals?)
        }

        // Convert record to map.
        Value::Record { val: record, .. } => {
            let pairs: Result<_, LabeledError> = record
                .into_iter()
                .map(|(k, v)| Ok((k.into(), nu_to_rmpv(v, compression)?)))
                .collect();

            rmpv::Value::Map(pairs?)
        }

        // Convert filesize to number of bytes, like `to json` does.
        Value::Filesize { val, .. } => val.into(),

        // Convert duration to nanoseconds, like `to json` does.
        Value::Duration { val, .. } => val.into(),

        // Convert date to msgpack extension type -1
        // defined in https://github.com/msgpack/msgpack/blob/master/spec.md
        Value::Date { val, .. } => {
            let nanos: u32 = val.timestamp_subsec_nanos();
            let seconds: i64 = val.timestamp();

            let mut data: Vec<u8>;

            // use the smallest datetime representation possible
            // TODO: implement 8 byte representation
            if let (Ok(seconds), 0) = (u32::try_from(seconds), nanos) {
                data = seconds.to_be_bytes().to_vec();
            } else {
                data = Vec::with_capacity(12);
                data.extend_from_slice(&nanos.to_be_bytes());
                data.extend_from_slice(&seconds.to_be_bytes());
            }
            rmpv::Value::Ext(-1, data)
        }
        Value::Range { val, .. } => {
            let vals: Result<_, _> = val
                .into_range_iter(span, None)
                .map(|r| nu_to_rmpv(r, compression))
                .collect();
            rmpv::Value::Array(vals?)
        }

        Value::Custom { val, internal_span } => {
            let val = val.to_base_value(internal_span)?;
            nu_to_rmpv(val, compression)?
        }

        Value::LazyRecord { val, .. } => nu_to_rmpv(val.collect()?, compression)?,

        // Convert anything we can't represent in msgpck to nil
        // Pretty sure this is how `to json` does it.
        _ => rmpv::Value::Nil,
        //Value::Block { val, .. } => todo!(),
        //Value::Closure { val, .. } => todo!(),
        //Value::Error { error, .. } => todo!(),
        //Value::CellPath { val, .. } => todo!(),
        //Value::MatchPattern { val, .. } => todo!(),
    })
}
