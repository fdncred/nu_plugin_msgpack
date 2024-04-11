use crate::MsgPackPlugin;
use nu_plugin::{EngineInterface, EvaluatedCall, SimplePluginCommand};
use nu_protocol::{
    record, Category, Example, LabeledError, Signature, Span, SyntaxShape, Type, Value,
};

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
                "brotli",
                SyntaxShape::Int,
                "Brotli Encoder Mode (0 - 11)",
                Some('b'),
            )
            .category(Category::Formats)
            .input_output_type(Type::Any, Type::Binary)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["msgpack", "plugins"]
    }

    fn examples(&self) -> Vec<Example> {
        vec![
            Example {
                example: "{ greeting: 'hello world' } | to msgpack",
                description: "Encode msgpack",
                result: Some(Value::test_binary(b"\x81\xA8\x67\x72\x65\x65\x74\x69\x6E\x67\xAB\x68\x65\x6C\x6C\x6F\x20\x77\x6F\x72\x6C\x64")),
            },
            Example {
                example: "{ greeting: 'hello world this is a long string' } | to msgpack --brotli 9",
                description: "Encode msgpack and compress strings with brotli (level 9)",
                result: Some(Value::test_binary(b"\x81\xA8\x67\x72\x65\x65\x74\x69\x6E\x67\xC4\x1E\x1B\x20\x00\x00\xA4\x40\xC2\x60\x22\x07\x0E\x51\xEB\x74\x70\xC8\xC9\xC1\x0E\x49\x4B\x63\xD3\xB1\x3C\x46\xC4\x4B\x4E\x69")),
            },
            Example {
                example: "{ greeting: 'hello world this is a long string' } | to msgpack --brotli 9 | from msgpack --brotli",
                description: "Encode and decode msgpack with brotli compressed strings",
                result: Some(Value::test_record(record! {
                    "greeting" => Value::test_string("hello world this is a long string")
                })),
            },
            Example {
                example: "{ hello: world } | save --raw helloworld.msgpack",
                description: "Save msgpack to a file",
                result: None,
            }
        ]
    }

    fn run(
        &self,
        _plugin: &MsgPackPlugin,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        input: &Value,
    ) -> Result<Value, LabeledError> {
        let compression: Option<i32> = call.get_flag::<i64>("brotli")?.map(|c| c as i32);
        let msgpack_value = nu_to_rmpv(input.clone(), compression)?;
        let mut encoded = vec![];
        rmpv::encode::write_value(&mut encoded, &msgpack_value)
            .expect("encoding to vec can't fail, right?");
        Ok(Value::binary(encoded, Span::unknown()))
    }
}

/// Convert [nu_protocol::Value] to a [rmpv::Value].
pub fn nu_to_rmpv(value: Value, compression: Option<i32>) -> Result<rmpv::Value, LabeledError> {
    let span = value.span();
    Ok(match value {
        Value::Bool { val, .. } => val.into(),
        Value::Int { val, .. } => val.into(),
        Value::Float { val, .. } => val.into(),
        Value::String { val, .. } => {
            if let Some(compression) = compression {
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
                nu_to_rmpv(bin, None)?
            } else {
                rmpv::Value::String(val.into())
            }
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

#[test]
fn test_examples() -> Result<(), nu_protocol::ShellError> {
    use nu_plugin_test_support::PluginTest;
    PluginTest::new("msgpack", MsgPackPlugin.into())?.test_command_examples(&ToMsgpack)
}
